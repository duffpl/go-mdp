package processor

import (
	"strings"
	"testing"

	"github.com/duffpl/go-mdp/v2/config"
	"github.com/duffpl/go-mdp/v2/faker"
)

func localeConfig(locale string) config.Config {
	return config.Config{
		Settings: config.Settings{Locale: locale},
		TableConfigs: []config.TableConfig{
			{
				TableName: "users",
				Columns: []config.ColumnConfig{
					{
						ColumnName: "first_name",
						Templates:  []config.Template{"{{ .FieldValue | transformFirstName }}"},
					},
				},
			},
		},
	}
}

// Processors created in one process share template text but not locale. Each
// one has to anonymize with the locale its own config asks for, no matter which
// locale compiled that template text first.
func TestProcessorUsesConfiguredLocale(t *testing.T) {
	const fieldValue = "John"
	fiName := faker.TransformFirstName(fieldValue, "fi")
	dkName := faker.TransformFirstName(fieldValue, "dk")
	if fiName == dkName {
		t.Fatalf("fi and dk both transform %q to %q, this test cannot tell the locales apart", fieldValue, fiName)
	}

	input := loadFixture(t, "users.sql")

	fiOutput, err := processSQL(t, localeConfig("fi"), input)
	if err != nil {
		t.Fatalf("cannot process with fi locale: %v", err)
	}
	dkOutput, err := processSQL(t, localeConfig("dk"), input)
	if err != nil {
		t.Fatalf("cannot process with dk locale: %v", err)
	}

	if !strings.Contains(fiOutput, fiName) {
		t.Errorf("expected fi name %q in output:\n%s", fiName, fiOutput)
	}
	if !strings.Contains(dkOutput, dkName) {
		t.Errorf("expected dk name %q in output, the processor used another locale:\n%s", dkName, dkOutput)
	}
}

// An unknown locale falls back to the default data set instead of panicking on
// an empty one.
func TestProcessorUnknownLocaleFallsBack(t *testing.T) {
	input := loadFixture(t, "users.sql")

	output, err := processSQL(t, localeConfig("xx"), input)
	if err != nil {
		t.Fatalf("cannot process with unknown locale: %v", err)
	}

	expected := faker.TransformFirstName("John", "default")
	if !strings.Contains(output, expected) {
		t.Errorf("expected default locale name %q in output:\n%s", expected, output)
	}
}
