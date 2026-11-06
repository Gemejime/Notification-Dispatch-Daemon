#!/usr/bin/env python3
"""
scripts/sync_env_to_deployments.py
==================================
Automated environment synchronization and deployment enforcement script.
Synchronizes non-sensitive environment variables from .env.prod into Helm values.yaml.

Usage:
  python scripts/sync_env_to_deployments.py --sync
  python scripts/sync_env_to_deployments.py --check-only
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yaml

# Support clean UTF-8 stdout/stderr on Windows environments
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Sensitive key substrings that must NEVER be committed to values.yaml
SENSITIVE_SUBSTRINGS = ("PASSWORD", "SECRET", "TOKEN", "KEY", "POSTGRES_DSN", "DSN")

# Pattern matching URL userinfo with embedded password (scheme://user:password@)
URL_PASSWORD_USERINFO_REGEX = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:[^/\s:@]+@")

DEFAULT_ENV_FILE = Path(".env.prod")
DEFAULT_VALUES_FILES = [
    Path("deployments/helm/values.yaml"),
    Path("deployments/helm/rag-notification-dispatch-daemon/values.yaml"),
]
DEFAULT_SECRET_NAME = "rag-notification-dispatch-daemon-secrets"


def is_sensitive_key(key: str) -> bool:
    """Return True if key contains any sensitive credentials keyword."""
    upper = key.upper()
    return any(sub in upper for sub in SENSITIVE_SUBSTRINGS)


def has_password_userinfo(val: str) -> bool:
    """Return True if val contains URL userinfo with a password, in the form scheme://user:password@."""
    return bool(URL_PASSWORD_USERINFO_REGEX.search(str(val)))


def parse_env_file(filepath: Path) -> Dict[str, str]:
    """Parse .env file into key-value pairs, ignoring empty lines and comments."""
    if not filepath.exists():
        raise FileNotFoundError(f"Environment file not found: {filepath}")

    env_vars: Dict[str, str] = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("export "):
                stripped = stripped[len("export "):].strip()
            if "=" not in stripped:
                continue

            key, raw_val = stripped.split("=", 1)
            key = key.strip()
            raw_val = raw_val.strip()

            # Handle quoted values
            if len(raw_val) >= 2 and (
                (raw_val.startswith('"') and raw_val.endswith('"'))
                or (raw_val.startswith("'") and raw_val.endswith("'"))
            ):
                val = raw_val[1:-1]
            else:
                # Strip inline unquoted comments: KEY=val # comment
                if " #" in raw_val:
                    val = raw_val.split(" #", 1)[0].strip()
                else:
                    val = raw_val

            env_vars[key] = val

    return env_vars


def get_target_values_files(custom_path: Optional[str] = None) -> List[Path]:
    """Return the list of values.yaml paths to sync or check."""
    if custom_path:
        return [Path(custom_path)]

    # Sync to both primary chart path and root helm path if present
    targets: List[Path] = []
    for p in DEFAULT_VALUES_FILES:
        if p.exists() or p.parent.exists():
            targets.append(p)

    if not targets:
        targets = [DEFAULT_VALUES_FILES[0]]
    return targets


def extract_existing_secret_name(values_content: str) -> str:
    """Extract existing secretName from values content, or return default."""
    try:
        parsed = yaml.safe_load(values_content)
        if isinstance(parsed, dict) and "env" in parsed and isinstance(parsed["env"], dict):
            sec = parsed["env"].get("secretName")
            if sec:
                return str(sec)
    except Exception:
        pass
    return DEFAULT_SECRET_NAME


def format_env_block(env_data: Dict[str, str], secret_name: str) -> str:
    """Format the env block for insertion into values.yaml."""
    lines = ["env:"]
    lines.append(f'  secretName: "{secret_name}"')
    for key in sorted(env_data.keys()):
        val = str(env_data[key])
        # Escape backslashes first, then quotes
        escaped_val = val.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'  {key}: "{escaped_val}"')
    return "\n".join(lines)


def update_values_content(original_content: str, env_data: Dict[str, str]) -> str:
    """
    Update the `env:` section in values.yaml while preserving all comments
    and other top-level Helm blocks (image, resources, keda, probes, etc.).
    """
    secret_name = extract_existing_secret_name(original_content)
    new_env_block = format_env_block(env_data, secret_name)

    # Check if `env:` section already exists
    env_match = re.search(r"^(env:\s*\n(?:[ \t]+[^\n]*\n|\s*\n)*)", original_content, re.MULTILINE)

    if env_match:
        # Replace the existing env block
        start, end = env_match.span()
        updated = original_content[:start] + new_env_block + "\n" + original_content[end:]
    else:
        # Insert before secret: or config: if present, otherwise append
        target_marker = re.search(r"^(secret:|config:)", original_content, re.MULTILINE)
        if target_marker:
            idx = target_marker.start()
            updated = (
                original_content[:idx]
                + "# 12-Factor runtime environment configuration (synced from .env.prod)\n"
                + new_env_block
                + "\n\n"
                + original_content[idx:]
            )
        else:
            updated = original_content.rstrip() + "\n\n" + new_env_block + "\n"

    # Verify that the generated YAML is syntactically valid
    parsed = yaml.safe_load(updated)
    if not isinstance(parsed, dict) or "env" not in parsed:
        raise ValueError("Failed to generate valid values.yaml structure containing 'env'")

    return updated


def sync_environments(env_file: Path, values_files: List[Path]) -> None:
    """Update values.yaml files from .env.prod, excluding sensitive keys."""
    raw_env = parse_env_file(env_file)
    non_sensitive_env = {
        k: v for k, v in raw_env.items()
        if not is_sensitive_key(k) and not has_password_userinfo(v)
    }
    sensitive_keys = [
        k for k in raw_env.keys()
        if is_sensitive_key(k) or has_password_userinfo(raw_env[k])
    ]

    print(f"[INFO] Parsed {len(raw_env)} variables from {env_file}")
    print(f"[INFO] Excluded {len(sensitive_keys)} sensitive variables: {', '.join(sorted(sensitive_keys))}")
    print(f"[INFO] Syncing {len(non_sensitive_env)} variables into Helm values.yaml")

    for v_file in values_files:
        v_file.parent.mkdir(parents=True, exist_ok=True)
        if v_file.exists():
            content = v_file.read_text(encoding="utf-8")
        else:
            # If creating a new values.yaml, look for base content from companion path
            companion = (
                Path("deployments/helm/rag-notification-dispatch-daemon/values.yaml")
                if "rag-notification-dispatch-daemon" not in str(v_file)
                else Path("deployments/helm/values.yaml")
            )
            if companion.exists():
                content = companion.read_text(encoding="utf-8")
            else:
                content = ""

        updated_content = update_values_content(content, non_sensitive_env)
        v_file.write_text(updated_content, encoding="utf-8")
        print(f"[OK] Successfully synchronized {v_file}")


def check_drift(env_file: Path, values_files: List[Path]) -> bool:
    """
    Check if any drift exists between .env.prod and values.yaml files.
    Returns True if clean (no drift), False if drift detected.
    """
    raw_env = parse_env_file(env_file)
    expected_env = {
        k: v for k, v in raw_env.items()
        if not is_sensitive_key(k) and not has_password_userinfo(v)
    }

    has_drift = False

    for v_file in values_files:
        if not v_file.exists():
            print(f"[DRIFT] Values file does not exist: {v_file}", file=sys.stderr)
            has_drift = True
            continue

        try:
            with open(v_file, "r", encoding="utf-8") as f:
                parsed = yaml.safe_load(f)
        except Exception as e:
            print(f"[DRIFT] Failed to parse {v_file} as YAML: {e}", file=sys.stderr)
            has_drift = True
            continue

        if not isinstance(parsed, dict) or "env" not in parsed:
            print(f"[DRIFT] Missing 'env' key in {v_file}", file=sys.stderr)
            has_drift = True
            continue

        current_env = parsed.get("env", {})
        if not isinstance(current_env, dict):
            print(f"[DRIFT] 'env' section in {v_file} is not a dictionary", file=sys.stderr)
            has_drift = True
            continue

        # Check for missing keys or mismatched values
        for key, expected_val in expected_env.items():
            if key not in current_env:
                print(f"[DRIFT] {v_file}: Missing variable {key} (expected: {expected_val})", file=sys.stderr)
                has_drift = True
            else:
                actual_val = str(current_env[key])
                if actual_val != str(expected_val):
                    print(
                        f"[DRIFT] {v_file}: Value mismatch for {key} (expected: {expected_val}, got: {actual_val})",
                        file=sys.stderr,
                    )
                    has_drift = True

        # Check for unexpected extra keys (ignoring secretName)
        for key in current_env.keys():
            if key == "secretName":
                continue
            if key not in expected_env:
                print(
                    f"[DRIFT] {v_file}: Extra variable {key} present in values.yaml but not in {env_file}",
                    file=sys.stderr,
                )
                has_drift = True

    return not has_drift


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync and validate environment variables between .env.prod and Helm values.yaml."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--sync",
        action="store_true",
        help="Synchronize .env.prod variables into deployments/helm/values.yaml",
    )
    group.add_argument(
        "--check-only",
        action="store_true",
        help="Verify zero drift between .env.prod and deployments/helm/values.yaml (exits with code 1 if drift detected)",
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=str(DEFAULT_ENV_FILE),
        help=f"Path to .env source file (default: {DEFAULT_ENV_FILE})",
    )
    parser.add_argument(
        "--values-file",
        type=str,
        default=None,
        help="Path to specific values.yaml file to check or sync",
    )

    args = parser.parse_args()

    env_path = Path(args.env_file)
    values_paths = get_target_values_files(args.values_file)

    if args.sync:
        try:
            sync_environments(env_path, values_paths)
            print("[SUCCESS] Environment sync complete. Zero drift.")
            sys.exit(0)
        except Exception as err:
            print(f"[ERROR] Sync failed: {err}", file=sys.stderr)
            sys.exit(1)

    if args.check_only:
        try:
            clean = check_drift(env_path, values_paths)
            if clean:
                print(f"[OK] Zero drift detected between {env_path} and Helm values.")
                sys.exit(0)
            else:
                print(
                    f"\n[DRIFT DETECTED] Helm values are out of sync with {env_path}.\n"
                    f"Please run: python scripts/sync_env_to_deployments.py --sync\n"
                    f"and stage the modified files before committing.",
                    file=sys.stderr,
                )
                sys.exit(1)
        except Exception as err:
            print(f"[ERROR] Drift check failed: {err}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()

