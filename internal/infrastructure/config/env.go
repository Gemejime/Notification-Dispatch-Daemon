// Package config provides 12-Factor environment configuration parsing,
// dynamic .env file discovery, and Go runtime memory/GC bound configuration.
package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

// LoadEnv loads environment variables from one or more .env files into the current process.
// If no filenames are specified, it checks ENV_FILE or searches for .env in the working directory
// and up to the repository root. Existing environment variables are overwritten unless
// DOTENV_OVERRIDE=false is set.
func LoadEnv(filenames ...string) error {
	override := true
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("DOTENV_OVERRIDE"))); v == "false" || v == "0" || v == "no" {
		override = false
	}
	return LoadEnvWithOverride(override, filenames...)
}

// LoadEnvWithOverride loads variables from files and specifies whether to override already-set env vars.
func LoadEnvWithOverride(override bool, filenames ...string) error {
	if len(filenames) == 0 {
		envPath := os.Getenv("ENV_FILE")
		if envPath == "" {
			cwd, err := os.Getwd()
			if err == nil {
				envPath = FindEnvFile(cwd)
			}
		}
		if envPath == "" {
			return nil // No .env file found; proceed silently with standard env / defaults
		}
		filenames = []string{envPath}
	}

	for _, filename := range filenames {
		f, err := os.Open(filename)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to open env file %s: %w", filename, err)
		}
		vars, parseErr := ParseEnv(f)
		_ = f.Close()
		if parseErr != nil {
			return fmt.Errorf("failed to parse env file %s: %w", filename, parseErr)
		}

		for k, v := range vars {
			if !override {
				if _, exists := os.LookupEnv(k); exists {
					continue
				}
			}
			if setErr := os.Setenv(k, v); setErr != nil {
				return fmt.Errorf("failed to set env var %s: %w", k, setErr)
			}
		}
	}
	return nil
}

// FindEnvFile traverses from startDir upwards to locate .env up to the repository boundary (where go.mod lives).
func FindEnvFile(startDir string) string {
	dir := startDir
	for i := 0; i < 6; i++ {
		candidate := filepath.Join(dir, ".env")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			// Found repository root; stop searching higher
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

// ParseEnv parses key-value pairs from an io.Reader representing a .env file.
func ParseEnv(r io.Reader) (map[string]string, error) {
	scanner := bufio.NewScanner(r)
	result := make(map[string]string)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Support "export KEY=value"
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		eqIdx := strings.Index(line, "=")
		if eqIdx == -1 {
			continue
		}

		key := strings.TrimSpace(line[:eqIdx])
		val := strings.TrimSpace(line[eqIdx+1:])

		if key == "" {
			continue
		}

		parsedVal := parseEnvValue(val)
		result[key] = parsedVal
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func parseEnvValue(raw string) string {
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		unquoted := raw[1 : len(raw)-1]
		return expandEscapes(unquoted)
	}
	if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
		return raw[1 : len(raw)-1]
	}

	// Unquoted: strip trailing inline comments separated by whitespace and '#'
	for i := 0; i < len(raw); i++ {
		if raw[i] == '#' && i > 0 && unicode.IsSpace(rune(raw[i-1])) {
			return strings.TrimSpace(raw[:i])
		}
	}
	return raw
}

func expandEscapes(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				sb.WriteByte('\n')
				i++
			case 'r':
				sb.WriteByte('\r')
				i++
			case 't':
				sb.WriteByte('\t')
				i++
			case '"':
				sb.WriteByte('"')
				i++
			case '\\':
				sb.WriteByte('\\')
				i++
			default:
				sb.WriteByte(s[i])
			}
		} else {
			sb.WriteByte(s[i])
		}
	}
	return sb.String()
}

// ParseMemoryLimitBytes parses human-readable memory limit strings (e.g. "1800MiB", "2GiB", "1000MB")
// into raw integer byte counts, falling back to defaultVal on error or empty input.
func ParseMemoryLimitBytes(raw string, defaultVal int64) int64 {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultVal
	}

	upper := strings.ToUpper(trimmed)
	var multiplier int64 = 1

	switch {
	case strings.HasSuffix(upper, "TIB") || strings.HasSuffix(upper, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		upper = strings.TrimSuffix(strings.TrimSuffix(upper, "TIB"), "TB")
	case strings.HasSuffix(upper, "GIB") || strings.HasSuffix(upper, "GB"):
		multiplier = 1024 * 1024 * 1024
		upper = strings.TrimSuffix(strings.TrimSuffix(upper, "GIB"), "GB")
	case strings.HasSuffix(upper, "MIB") || strings.HasSuffix(upper, "MB"):
		multiplier = 1024 * 1024
		upper = strings.TrimSuffix(strings.TrimSuffix(upper, "MIB"), "MB")
	case strings.HasSuffix(upper, "KIB") || strings.HasSuffix(upper, "KB"):
		multiplier = 1024
		upper = strings.TrimSuffix(strings.TrimSuffix(upper, "KIB"), "KB")
	case strings.HasSuffix(upper, "B"):
		multiplier = 1
		upper = strings.TrimSuffix(upper, "B")
	}

	upper = strings.TrimSpace(upper)
	val, err := strconv.ParseInt(upper, 10, 64)
	if err != nil || val <= 0 {
		return defaultVal
	}
	return val * multiplier
}

// ParseGCPercent parses GOGC values ("80", "100", "off"), falling back to defaultVal on error or empty input.
func ParseGCPercent(raw string, defaultVal int) int {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultVal
	}
	if strings.EqualFold(trimmed, "off") {
		return -1
	}
	val, err := strconv.Atoi(trimmed)
	if err != nil || val < -1 {
		return defaultVal
	}
	return val
}

