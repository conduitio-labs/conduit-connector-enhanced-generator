package internal

import (
	"encoding/json"
	"encoding/xml"
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

func TestGenerateFHIRPatient(t *testing.T) {
	g := NewGenerator(0)
	patient, err := g.GenerateFHIRPatient()

	if err != nil {
		t.Fatalf("GenerateFHIRPatient failed: %v", err)
	}

	// Verify all required fields are present
	if patient.ID == "" {
		t.Error("Patient ID is empty")
	}

	if len(patient.Name) == 0 {
		t.Error("Patient has no names")
	} else {
		if len(patient.Name[0].Family) == 0 {
			t.Error("Patient has no family name")
		}
		if len(patient.Name[0].Given) == 0 {
			t.Error("Patient has no given name")
		}
	}

	if patient.BirthDate == "" {
		t.Error("Patient has no birth date")
	}

	if patient.Gender == "" {
		t.Error("Patient has no gender")
	}

	if len(patient.Address) == 0 {
		t.Error("Patient has no addresses")
	} else {
		addr := patient.Address[0]
		if len(addr.Line) == 0 {
			t.Error("Address has no street line")
		}
		if addr.City == "" {
			t.Error("Address has no city")
		}
		if addr.State == "" {
			t.Error("Address has no state")
		}
		if addr.PostalCode == "" {
			t.Error("Address has no postal code")
		}
		if addr.Country == "" {
			t.Error("Address has no country")
		}
	}
}

func TestNewFHIRPatientRecordGenerator(t *testing.T) {
	generator, err := NewFHIRPatientRecordGenerator(
		"patients",
		[]opencdc.Operation{opencdc.OperationCreate},
	)
	require.NoError(t, err)

	record := generator.Next()

	// Check record metadata
	assert.Equal(t, "patients", record.Metadata["collection"])
	assert.Equal(t, opencdc.OperationCreate, record.Operation)

	// Verify the payload can be unmarshaled into a FHIRPatient
	var patient FHIRPatient
	err = json.Unmarshal(record.Payload.After.(opencdc.RawData), &patient)
	require.NoError(t, err)

	// Verify required fields
	assert.NotEmpty(t, patient.ID)
	assert.NotEmpty(t, patient.Name)
	assert.NotEmpty(t, patient.BirthDate)
	assert.NotEmpty(t, patient.Gender)
	assert.NotEmpty(t, patient.Address)
}

func TestGenerateHL7v3Message(t *testing.T) {
	g := NewGenerator(0)
	message, err := g.GenerateHL7v3Message()
	require.NoError(t, err)

	// Verify XML structure
	var patient HL7v3Patient
	err = xml.Unmarshal(message, &patient)
	require.NoError(t, err, "Generated XML should be valid")

	// Verify required fields
	assert.True(t, patient.ID >= 0 && patient.ID <= 9999,
		"ID should be between 0 and 9999, got %d", patient.ID)
	assert.NotEmpty(t, patient.Name)
	assert.NotEmpty(t, patient.Name[0].Given)
	assert.NotEmpty(t, patient.Name[0].Family)
	assert.Contains(t, []string{"M", "F"}, patient.Gender)
	assert.Regexp(t, `^\d{14}$`, patient.BirthTime) // YYYYMMDDHHMMSS format

	// Verify address components
	if assert.NotEmpty(t, patient.Address) {
		addr := patient.Address[0]
		assert.NotEmpty(t, addr.Street)
		assert.NotEmpty(t, addr.City)
		assert.NotEmpty(t, addr.State)
		assert.NotEmpty(t, addr.ZipCode)
	}
}

func TestNewHL7v3RecordGenerator(t *testing.T) {
	generator, err := NewHL7v3RecordGenerator(
		"hl7v3_patients",
		[]opencdc.Operation{opencdc.OperationCreate},
	)
	require.NoError(t, err)

	record := generator.Next()

	// Check record metadata
	assert.Equal(t, "hl7v3_patients", record.Metadata["collection"])
	assert.Equal(t, opencdc.OperationCreate, record.Operation)

	// Verify the payload is valid XML
	var patient HL7v3Patient
	err = xml.Unmarshal(record.Payload.After.(opencdc.RawData), &patient)
	require.NoError(t, err)

	// Verify namespace and root element
	assert.Equal(t, "Patient", patient.XMLName.Local)
	assert.Equal(t, "urn:hl7-org:v3", patient.XMLName.Space)

	// Verify ID is numeric
	assert.True(t, patient.ID >= 0 && patient.ID <= 9999,
		"ID should be between 0 and 9999, got %d", patient.ID)
}
