package internal

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomStructuredData(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]string
		check  func(t *testing.T, data opencdc.StructuredData)
	}{
		{
			name: "Basic types",
			fields: map[string]string{
				"intField":      "int",
				"stringField":   "string",
				"timeField":     "time",
				"durationField": "duration",
				"boolField":     "bool",
			},
			check: func(t *testing.T, data opencdc.StructuredData) {
				assert.IsType(t, int(0), data["intField"])
				assert.IsType(t, "", data["stringField"])
				assert.IsType(t, time.Time{}, data["timeField"])
				assert.IsType(t, time.Duration(0), data["durationField"])
				assert.IsType(t, bool(false), data["boolField"])
			},
		},
		{
			name: "New types",
			fields: map[string]string{
				"nameField":       TypeName,
				"emailField":      TypeEmail,
				"employeeIDField": TypeEmployeeID,
				"ssnField":        TypeSSN,
				"creditCardField": TypeCreditCard,
				"orderNumField":   TypeOrderNum,
			},
			check: func(t *testing.T, data opencdc.StructuredData) {
				assert.Regexp(t, `^[A-Z][a-z]+ [A-Z][a-z]+$`, data["nameField"])
				assert.Regexp(t, `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`, data["emailField"])
				assert.Regexp(t, `^EMP\d{4}$`, data["employeeIDField"])
				assert.Regexp(t, `^XXX-XX-\d{4}$`, data["ssnField"])
				assert.Regexp(t, `^X{12,}\d{4}$`, data["creditCardField"])
				assert.Regexp(t, `^ORD-[a-f0-9-]{36}$`, data["orderNumField"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := randomStructuredData(tt.fields)
			require.IsType(t, opencdc.StructuredData{}, data)
			tt.check(t, data.(opencdc.StructuredData))
		})
	}
}

func TestRandomRawData(t *testing.T) {
	fields := map[string]string{
		"nameField":       TypeName,
		"emailField":      TypeEmail,
		"employeeIDField": TypeEmployeeID,
		"ssnField":        TypeSSN,
		"creditCardField": TypeCreditCard,
		"orderNumField":   TypeOrderNum,
	}

	rawData := randomRawData(fields)
	require.IsType(t, opencdc.RawData{}, rawData)

	// Attempt to unmarshal the raw data
	var structuredData map[string]interface{}
	err := json.Unmarshal(rawData, &structuredData)
	require.NoError(t, err)

	// Check if all fields are present
	for field := range fields {
		assert.Contains(t, structuredData, field)
	}
}
