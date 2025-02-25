// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"encoding/xml"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/goccy/go-json"
)

// Add new constants for specific string types
const (
	TypeName       = "name"
	TypeEmail      = "email"
	TypeEmployeeID = "employeeid"
	TypeSSN        = "ssn"
	TypeCreditCard = "creditcard"
	TypeOrderNum   = "ordernumber"
)

// Update the KnownTypes slice to include the new types
var KnownTypes = []string{
	"int", "string", "time", "bool", "duration",
	TypeName, TypeEmail, TypeEmployeeID, TypeSSN, TypeCreditCard, TypeOrderNum,
}

// RecordGenerator is an interface for generating records.
type RecordGenerator interface {
	// Next generates the next record.
	Next() opencdc.Record
}

type baseRecordGenerator struct {
	collection   string
	operations   []opencdc.Operation
	generateData func() opencdc.Data

	count int
}

func (g *baseRecordGenerator) Next() opencdc.Record {
	g.count++

	metadata := make(opencdc.Metadata)
	metadata.SetCreatedAt(time.Now())
	if g.collection != "" {
		metadata["collection"] = g.collection
	}

	rec := opencdc.Record{
		Position:  opencdc.Position(strconv.Itoa(g.count)),
		Operation: g.operations[rand.Intn(len(g.operations))],
		Metadata:  metadata,
		Key:       opencdc.RawData(randomWord()),
	}

	switch rec.Operation {
	case opencdc.OperationSnapshot, opencdc.OperationCreate:
		rec.Payload.After = g.generateData()
	case opencdc.OperationUpdate:
		rec.Payload.Before = g.generateData()
		rec.Payload.After = g.generateData()
	case opencdc.OperationDelete:
		rec.Payload.Before = g.generateData()
	}

	return rec
}

// NewFileRecordGenerator creates a RecordGenerator that reads the contents of a
// file at the given path. The file is read once and cached in memory. The
// RecordGenerator will generate records with the contents of the file as the
// payload data.
func NewFileRecordGenerator(
	collection string,
	operations []opencdc.Operation,
	path string,
) (RecordGenerator, error) {
	// Files are cached, so that the time to read files doesn't affect generator
	// read times and the message rate. This will increase Conduit's memory usage.
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			return opencdc.RawData(bytes)
		},
	}, nil
}

// NewStructuredRecordGenerator creates a RecordGenerator that generates records
// with structured data. The fields map should contain the field names and types
// for the structured data. The types can be one of: int, string, time, bool.
func NewStructuredRecordGenerator(
	collection string,
	operations []opencdc.Operation,
	fields map[string]string,
) (RecordGenerator, error) {
	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			return randomStructuredData(fields)
		},
	}, nil
}

// NewRawRecordGenerator creates a RecordGenerator that generates records with
// raw data. The fields map should contain the field names and types for the raw
// data. The types can be one of: int, string, time, bool.
func NewRawRecordGenerator(
	collection string,
	operations []opencdc.Operation,
	fields map[string]string,
) (RecordGenerator, error) {
	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			return randomRawData(fields)
		},
	}, nil
}

func randomStructuredData(fields map[string]string) opencdc.Data {
	data := make(opencdc.StructuredData)
	for field, typ := range fields {
		switch typ {
		case "int":
			data[field] = rand.Int()
		case "string":
			data[field] = randomWord()
		case "time":
			data[field] = time.Now().UTC()
		case "duration":
			data[field] = time.Duration(rand.Intn(1000)) * time.Second
		case "bool":
			data[field] = rand.Int()%2 == 0
		case TypeName:
			data[field] = gofakeit.Name()
		case TypeEmail:
			data[field] = gofakeit.Email()
		case TypeEmployeeID:
			data[field] = fmt.Sprintf("EMP%d", gofakeit.Number(1000, 9999))
		case TypeSSN:
			// Format as XXX-XX-1234 where only last 4 digits are visible
			lastFour := fmt.Sprintf("%04d", gofakeit.Number(0, 9999))
			data[field] = fmt.Sprintf("XXX-XX-%s", lastFour)
		case TypeCreditCard:
			// Format as XXXXXXXXXXXX1234 where only last 4 digits are visible
			lastFour := fmt.Sprintf("%04d", gofakeit.Number(0, 9999))
			data[field] = fmt.Sprintf("XXXXXXXXXXXX%s", lastFour)
		case TypeOrderNum:
			data[field] = fmt.Sprintf("ORD-%s", gofakeit.UUID())
		default:
			panic(fmt.Errorf("field %q contains invalid type: %v", field, typ))
		}
	}
	return data
}

func randomRawData(fields map[string]string) opencdc.RawData {
	data := randomStructuredData(fields)
	bytes, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Errorf("couldn't serialize data: %w", err))
	}
	return bytes
}

// Generator handles the generation of random data
type Generator struct {
	rand *rand.Rand
	// Add counter for unique IDs
	patientIDCounter int
}

// NewGenerator creates a new Generator with the given seed
func NewGenerator(seed int64) *Generator {
	return &Generator{
		rand:             rand.New(rand.NewSource(seed)),
		patientIDCounter: 0,
	}
}

// Helper methods for the Generator
func (g *Generator) firstName() string {
	return gofakeit.FirstName()
}

func (g *Generator) lastName() string {
	return gofakeit.LastName()
}

func (g *Generator) streetAddress() string {
	return gofakeit.Street()
}

func (g *Generator) city() string {
	return gofakeit.City()
}

func (g *Generator) state() string {
	return gofakeit.State()
}

func (g *Generator) zipCode() string {
	return gofakeit.Zip()
}

func (g *Generator) gender() string {
	genders := []string{"male", "female"}
	return genders[g.rand.Intn(len(genders))]
}

// FHIRPatient represents a FHIR patient resource
type FHIRPatient struct {
	ID   string `json:"id"`
	Name []struct {
		Family []string `json:"family"`
		Given  []string `json:"given"`
	} `json:"name"`
	BirthDate string `json:"birthDate"`
	Gender    string `json:"gender"`
	Address   []struct {
		Line       []string `json:"line"`
		City       string   `json:"city"`
		State      string   `json:"state"`
		PostalCode string   `json:"postalCode"`
		Country    string   `json:"country"`
	} `json:"address"`
}

// GenerateFHIRPatient creates a new FHIR patient with random but realistic data
func (g *Generator) GenerateFHIRPatient() (*FHIRPatient, error) {
	// Increment counter for unique ID
	g.patientIDCounter++

	patient := &FHIRPatient{
		// Use counter for ID instead of random number
		ID: fmt.Sprintf("%d", g.patientIDCounter),
		Name: []struct {
			Family []string `json:"family"`
			Given  []string `json:"given"`
		}{
			{
				Family: []string{g.lastName()},
				Given:  []string{g.firstName()},
			},
		},
		Gender: g.gender(),
		Address: []struct {
			Line       []string `json:"line"`
			City       string   `json:"city"`
			State      string   `json:"state"`
			PostalCode string   `json:"postalCode"`
			Country    string   `json:"country"`
		}{
			{
				Line:       []string{g.streetAddress()},
				City:       g.city(),
				State:      g.state(),
				PostalCode: g.zipCode(),
				Country:    "USA",
			},
		},
	}

	// Generate a random birthdate between 1920 and 2020
	year := g.rand.Intn(100) + 1920
	month := g.rand.Intn(12) + 1
	day := g.rand.Intn(28) + 1 // Using 28 to avoid invalid dates
	patient.BirthDate = fmt.Sprintf("%04d-%02d-%02d", year, month, day)

	return patient, nil
}

// NewFHIRPatientRecordGenerator creates a RecordGenerator that generates FHIR patient records
func NewFHIRPatientRecordGenerator(
	collection string,
	operations []opencdc.Operation,
) (RecordGenerator, error) {
	generator := NewGenerator(time.Now().UnixNano())

	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			patient, err := generator.GenerateFHIRPatient()
			if err != nil {
				panic(fmt.Errorf("failed to generate FHIR patient: %w", err))
			}

			// Convert to JSON
			bytes, err := json.Marshal(patient)
			if err != nil {
				panic(fmt.Errorf("failed to marshal FHIR patient: %w", err))
			}

			return opencdc.RawData(bytes)
		},
	}, nil
}

// HL7Message represents an HL7 message structure
type HL7Message struct {
	MSH MSHSegment
	PID PIDSegment
}

type MSHSegment struct {
	SendingApplication   string
	SendingFacility      string
	ReceivingApplication string
	ReceivingFacility    string
	DateTime             time.Time
	MessageType          string
	MessageControlID     string
	ProcessingID         string
	Version              string
}

type PIDSegment struct {
	SetID       string
	PatientID   string
	PatientName string
	DateOfBirth string
	Gender      string
	Address     string
}

// GenerateHL7Message creates a new HL7 message with random but realistic data
func (g *Generator) GenerateHL7Message() (string, error) {
	now := time.Now()
	// Increment counter for unique ID
	g.patientIDCounter++

	msh := MSHSegment{
		SendingApplication:   "FHIR_CONVERTER",
		SendingFacility:      "FACILITY",
		ReceivingApplication: "HL7_PARSER",
		ReceivingFacility:    "FACILITY",
		DateTime:             now,
		MessageType:          "ADT^A01",
		MessageControlID:     now.Format("20060102150405"),
		ProcessingID:         "P",
		Version:              "2.5",
	}

	pid := PIDSegment{
		SetID: "1",
		// Use counter for PatientID instead of random number
		PatientID:   fmt.Sprintf("%d", g.patientIDCounter),
		PatientName: fmt.Sprintf("%s^%s", g.lastName(), g.firstName()),
		DateOfBirth: time.Date(1920+g.rand.Intn(100), time.Month(1+g.rand.Intn(12)), 1+g.rand.Intn(28), 0, 0, 0, 0, time.UTC).Format("2006-01-02"),
		Gender:      g.gender(),
		Address:     fmt.Sprintf("%s^%s^%s^%s^USA", g.streetAddress(), g.city(), g.state(), g.zipCode()),
	}

	// Format the HL7 message
	message := fmt.Sprintf(
		"MSH|^~\\&|%s|%s|%s|%s|%s||%s|%s|%s|%s|\n"+
			"PID|%s||%s||%s||%s|%s|||%s||||||%s",
		msh.SendingApplication, msh.SendingFacility, msh.ReceivingApplication, msh.ReceivingFacility,
		msh.DateTime.Format("20060102150405"), msh.MessageType, msh.MessageControlID, msh.ProcessingID, msh.Version,
		pid.SetID, pid.PatientID, pid.PatientName, pid.DateOfBirth, pid.Gender, pid.Address, pid.PatientID,
	)

	return message, nil
}

// NewHL7RecordGenerator creates a RecordGenerator that generates HL7 messages
func NewHL7RecordGenerator(
	collection string,
	operations []opencdc.Operation,
) (RecordGenerator, error) {
	generator := NewGenerator(time.Now().UnixNano())

	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			message, err := generator.GenerateHL7Message()
			if err != nil {
				panic(fmt.Errorf("failed to generate HL7 message: %w", err))
			}

			return opencdc.RawData(message)
		},
	}, nil
}

// HL7v3Patient represents an HL7 v3 Patient structure in XML format
type HL7v3Patient struct {
	XMLName xml.Name `xml:"urn:hl7-org:v3 Patient"`
	ID      int      `xml:"id"`
	Name    []struct {
		Given  []string `xml:"given"`
		Family string   `xml:"family"`
	} `xml:"name"`
	Gender    string `xml:"administrativeGenderCode>code"`
	BirthTime string `xml:"birthTime>value"`
	Address   []struct {
		Street  []string `xml:"streetAddressLine"`
		City    string   `xml:"city"`
		State   string   `xml:"state"`
		ZipCode string   `xml:"postalCode"`
	} `xml:"addr"`
}

// GenerateHL7v3Message creates a new HL7 v3 XML message
func (g *Generator) GenerateHL7v3Message() ([]byte, error) {
	// Increment counter for unique ID
	g.patientIDCounter++

	patient := &HL7v3Patient{
		// Use counter for ID instead of random number
		ID: g.patientIDCounter,
		Gender: func() string {
			if g.gender() == "male" {
				return "M"
			}
			return "F"
		}(),
		BirthTime: time.Date(
			1950+g.rand.Intn(50),
			time.Month(1+g.rand.Intn(12)),
			1+g.rand.Intn(28),
			0, 0, 0, 0, time.UTC,
		).Format("20060102150405"),
	}

	// Generate name
	name := struct {
		Given  []string `xml:"given"`
		Family string   `xml:"family"`
	}{
		Given:  []string{g.firstName()},
		Family: g.lastName(),
	}
	patient.Name = append(patient.Name, name)

	// Generate address
	address := struct {
		Street  []string `xml:"streetAddressLine"`
		City    string   `xml:"city"`
		State   string   `xml:"state"`
		ZipCode string   `xml:"postalCode"`
	}{
		Street:  []string{g.streetAddress()},
		City:    g.city(),
		State:   g.state(),
		ZipCode: g.zipCode(),
	}
	patient.Address = append(patient.Address, address)

	// Generate XML with proper namespaces
	output := []byte(`<?xml version="1.0" encoding="UTF-8"?>`)
	xmlData, err := xml.MarshalIndent(patient, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal HL7 v3 XML: %w", err)
	}
	output = append(output, xmlData...)

	return output, nil
}

// NewHL7v3RecordGenerator creates a RecordGenerator for HL7 v3 messages
func NewHL7v3RecordGenerator(
	collection string,
	operations []opencdc.Operation,
) (RecordGenerator, error) {
	generator := NewGenerator(time.Now().UnixNano())

	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			message, err := generator.GenerateHL7v3Message()
			if err != nil {
				panic(fmt.Errorf("failed to generate HL7 v3 message: %w", err))
			}
			return opencdc.RawData(message)
		},
	}, nil
}
