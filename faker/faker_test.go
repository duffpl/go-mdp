package faker

import (
	"regexp"
	"testing"
)

func TestTransformBusinessId(t *testing.T) {
	tests := []struct {
		locale  string
		pattern string
		desc    string
	}{
		{"fi", `^\d{7}-\d$`, "Finnish Y-tunnus (7 digits-1 digit)"},
		{"no", `^\d{9}$`, "Norwegian Organisasjonsnummer (9 digits)"},
		{"dk", `^\d{8}$`, "Danish CVR (8 digits)"},
		{"se", `^\d{6}-\d{4}$`, "Swedish Organisationsnummer (6 digits-4 digits)"},
		{"default", `^\d{9}$`, "Default fallback (9 digits)"},
	}

	testInput := "test-company-123"

	for _, tt := range tests {
		t.Run(tt.locale, func(t *testing.T) {
			result := TransformBusinessId(testInput, tt.locale)
			matched, err := regexp.MatchString(tt.pattern, result)
			if err != nil {
				t.Fatalf("Invalid regex pattern: %v", err)
			}
			if !matched {
				t.Errorf("%s failed: got %q, expected pattern %q", tt.desc, result, tt.pattern)
			}
		})
	}
}

func TestTransformBusinessIdDeterministic(t *testing.T) {
	input := "test-company-456"
	locale := "fi"

	// Generate business ID multiple times with the same input
	result1 := TransformBusinessId(input, locale)
	result2 := TransformBusinessId(input, locale)
	result3 := TransformBusinessId(input, locale)

	if result1 != result2 || result2 != result3 {
		t.Errorf("TransformBusinessId is not deterministic: got %q, %q, %q", result1, result2, result3)
	}
}

func TestTransformBusinessIdDifferentInputs(t *testing.T) {
	locale := "se"
	input1 := "company-a"
	input2 := "company-b"

	result1 := TransformBusinessId(input1, locale)
	result2 := TransformBusinessId(input2, locale)

	if result1 == result2 {
		t.Errorf("Different inputs should produce different outputs: both got %q", result1)
	}
}

func TestTransformBusinessIdEmptyInput(t *testing.T) {
	result := TransformBusinessId("", "fi")
	if result != "" {
		t.Errorf("Empty input should return empty string, got %q", result)
	}
}
