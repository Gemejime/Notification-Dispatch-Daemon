package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/config"
)

func TestParseEnv(t *testing.T) {
	input := `
# Comment line
NODE_ID=node-test-01
HTTP_PORT=9090
LOG_LEVEL="DEBUG"
DOUBLE_QUOTED="hello\nworld\ttest"
SINGLE_QUOTED='raw$value#notacomment'
INLINE_COMMENT=value123 # this is a comment
export EXPORTED_VAR=exported_value
EMPTY_LINE=

KEY_WITH_EQUALS=foo=bar=baz
`
	vars, err := config.ParseEnv(strings.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error parsing env: %v", err)
	}

	expected := map[string]string{
		"NODE_ID":         "node-test-01",
		"HTTP_PORT":       "9090",
		"LOG_LEVEL":       "DEBUG",
		"DOUBLE_QUOTED":   "hello\nworld\ttest",
		"SINGLE_QUOTED":   "raw$value#notacomment",
		"INLINE_COMMENT":  "value123",
		"EXPORTED_VAR":    "exported_value",
		"KEY_WITH_EQUALS": "foo=bar=baz",
	}

	for k, want := range expected {
		got, ok := vars[k]
		if !ok {
			t.Errorf("expected key %q to be present", k)
			continue
		}
		if got != want {
			t.Errorf("key %q: got %q, want %q", k, got, want)
		}
	}
}

func TestParseMemoryLimitBytes(t *testing.T) {
	tests := []struct {
		raw      string
		defaultV int64
		want     int64
	}{
		{"1800MiB", 0, 1800 * 1024 * 1024},
		{"2GiB", 0, 2 * 1024 * 1024 * 1024},
		{"512KiB", 0, 512 * 1024},
		{"1TiB", 0, 1024 * 1024 * 1024 * 1024},
		{"1000MB", 0, 1000 * 1024 * 1024},
		{"1073741824", 0, 1073741824},
		{"", 1887436800, 1887436800},
		{"invalid", 1887436800, 1887436800},
		{"-50MiB", 1887436800, 1887436800},
	}

	for _, tt := range tests {
		got := config.ParseMemoryLimitBytes(tt.raw, tt.defaultV)
		if got != tt.want {
			t.Errorf("ParseMemoryLimitBytes(%q): got %d, want %d", tt.raw, got, tt.want)
		}
	}
}

func TestParseGCPercent(t *testing.T) {
	tests := []struct {
		raw      string
		defaultV int
		want     int
	}{
		{"80", 80, 80},
		{"100", 80, 100},
		{"off", 80, -1},
		{"OFF", 80, -1},
		{"-1", 80, -1},
		{"", 80, 80},
		{"invalid", 80, 80},
		{"-5", 80, 80},
	}

	for _, tt := range tests {
		got := config.ParseGCPercent(tt.raw, tt.defaultV)
		if got != tt.want {
			t.Errorf("ParseGCPercent(%q): got %d, want %d", tt.raw, got, tt.want)
		}
	}
}

func TestLoadEnvFileAndOverride(t *testing.T) {
	tmpDir := t.TempDir()
	envPath := filepath.Join(tmpDir, ".env")

	content := `
HTTP_PORT=9999
LOG_LEVEL=WARN
TEST_DYNAMIC_VAR=hello_from_env
`
	if err := os.WriteFile(envPath, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write tmp env file: %v", err)
	}

	// 1. Load with override = true
	t.Setenv("HTTP_PORT", "8080")
	if err := config.LoadEnvWithOverride(true, envPath); err != nil {
		t.Fatalf("unexpected error loading env: %v", err)
	}

	if got := os.Getenv("HTTP_PORT"); got != "9999" {
		t.Errorf("expected HTTP_PORT to be overridden to 9999, got %q", got)
	}
	if got := os.Getenv("LOG_LEVEL"); got != "WARN" {
		t.Errorf("expected LOG_LEVEL to be WARN, got %q", got)
	}
	if got := os.Getenv("TEST_DYNAMIC_VAR"); got != "hello_from_env" {
		t.Errorf("expected TEST_DYNAMIC_VAR to be hello_from_env, got %q", got)
	}

	// 2. Load with override = false: existing HTTP_PORT should be preserved
	t.Setenv("HTTP_PORT", "7777")
	if err := config.LoadEnvWithOverride(false, envPath); err != nil {
		t.Fatalf("unexpected error loading env without override: %v", err)
	}
	if got := os.Getenv("HTTP_PORT"); got != "7777" {
		t.Errorf("expected HTTP_PORT to remain 7777 with override=false, got %q", got)
	}
}
