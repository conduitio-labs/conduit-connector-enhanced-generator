// Copyright © 2022 Meroxa, Inc.
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

package generator

import (
	"context"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-enhanced-generator/internal"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/goccy/go-json"
	"github.com/matryer/is"
)

func TestSource_Read_RawData(t *testing.T) {
	is := is.New(t)
	underTest := openTestSource(
		t,
		map[string]string{
			"recordCount":            "1",
			"format.type":            "raw",
			"format.options.id":      "int",
			"format.options.name":    "string",
			"format.options.joined":  "time",
			"format.options.admin":   "bool",
			"format.options.timeout": "duration",

			"operations": "delete",
		},
	)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)
	now := time.Now()

	v, ok := rec.Payload.Before.(opencdc.RawData)
	is.True(ok)

	recMap := make(map[string]any)
	err = json.Unmarshal(v, &recMap)
	is.NoErr(err)

	is.Equal(len(recMap), 5)
	is.True(recMap["id"].(float64) > 0)
	is.True(recMap["name"].(string) != "")
	_, ok = recMap["admin"].(bool)
	is.True(ok)
	dur, ok := recMap["timeout"].(float64)
	is.True(ok)
	is.True(dur > 0)

	ts := recMap["joined"].(string)
	joined, err := time.Parse(time.RFC3339Nano, ts)
	is.NoErr(err)
	is.True(!joined.After(now))
	is.True(joined.After(now.Add(-time.Millisecond * 10)))
}

func TestSource_Read_PayloadFile(t *testing.T) {
	is := is.New(t)
	underTest := openTestSource(
		t,
		map[string]string{
			"recordCount":         "1",
			"format.type":         "file",
			"format.options.path": "./source_test.go",
			"operations":          "update",
		},
	)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)

	v, ok := rec.Payload.After.(opencdc.RawData)
	is.True(ok)

	expected, err := os.ReadFile("./source_test.go")
	is.NoErr(err)
	is.Equal(expected, v.Bytes())
}

func TestSource_Read_StructuredData(t *testing.T) {
	is := is.New(t)
	underTest := openTestSource(
		t,
		map[string]string{
			"recordCount":            "1",
			"format.type":            "structured",
			"format.options.id":      "int",
			"format.options.name":    "string",
			"format.options.joined":  "time",
			"format.options.admin":   "bool",
			"format.options.timeout": "duration",
			"operations":             "snapshot",
		},
	)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)
	now := time.Now()

	v, ok := rec.Payload.After.(opencdc.StructuredData)
	is.True(ok)

	is.Equal(len(v), 5)
	is.True(v["id"].(int) > 0)
	is.True(v["name"].(string) != "")
	_, ok = v["admin"].(bool)
	is.True(ok)
	dur, ok := v["timeout"].(time.Duration)
	is.True(ok)
	is.True(dur > 0)

	joined, ok := v["joined"].(time.Time)
	is.True(ok)
	is.True(!joined.After(now))
	is.True(joined.After(now.Add(-time.Millisecond * 10)))
}

func TestSource_Read_RateLimit(t *testing.T) {
	cfg := map[string]string{
		"burst.sleepTime":    "100ms",
		"burst.generateTime": "150ms",
		"format.type":        "raw",
		"format.options.id":  "int",
		"operations":         "create,update",
	}

	// Test rate parameter
	t.Run("parameter-rate", func(t *testing.T) {
		cfg := maps.Clone(cfg)
		cfg["rate"] = "20"
		testSourceRateLimit(t, cfg)
	})
	// Test readTime parameter
	t.Run("parameter-readTime", func(t *testing.T) {
		cfg := maps.Clone(cfg)
		cfg["readTime"] = "50ms"
		testSourceRateLimit(t, cfg)
	})
}

func testSourceRateLimit(t *testing.T, cfg map[string]string) {
	ctx := context.Background()

	underTest := openTestSource(t, cfg)

	const epsilon = time.Millisecond * 10
	readAssertDelay := func(is *is.I, expectedDelay time.Duration) {
		is.Helper()
		start := time.Now()
		_, err := underTest.Read(ctx)
		dur := time.Since(start)
		is.NoErr(err)
		is.True(dur >= expectedDelay-epsilon) // expected longer delay
		is.True(dur <= expectedDelay+epsilon) // expected shorter delay
	}

	is := is.New(t)

	// We start in the generate cycle, we can test the rate limiting here.
	// The first record should be read immediately.
	readAssertDelay(is, 0)

	// The second record should already be rate limited and delayed by 50ms.
	readAssertDelay(is, 50*time.Millisecond)

	// If we wait for 50ms before reading, the next record should be read immediately.
	time.Sleep(50 * time.Millisecond)
	readAssertDelay(is, 0)

	// If we wait for 25ms, the next record should be read after 25ms.
	time.Sleep(25 * time.Millisecond)
	readAssertDelay(is, 25*time.Millisecond)

	// By now we should have reached the end of burst.generateTime (150ms).
	// If we try to read a record now we should have to wait for 100ms (burst.sleepTime).
	readAssertDelay(is, 100*time.Millisecond)

	// After the sleep cycle we are again in the generate cycle. Reading a record
	// should have the normal delay of 50ms.
	readAssertDelay(is, 50*time.Millisecond)

	// Wait for 100ms (remaining generate time) + 50ms (half of sleep time) = 150ms,
	// so we are in the middle of the sleep cycle. Reading at that point should
	// take 50ms.
	time.Sleep(150 * time.Millisecond)
	readAssertDelay(is, 50*time.Millisecond)
}

func TestSource_Read_FHIRData(t *testing.T) {
	is := is.New(t)
	underTest := openTestSource(
		t,
		map[string]string{
			"recordCount": "1",
			"format.type": "fhir",
			"operations":  "create",
		},
	)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)

	// Check that we got raw data (JSON)
	v, ok := rec.Payload.After.(opencdc.RawData)
	is.True(ok)

	// Unmarshal into FHIRPatient to verify structure
	var patient internal.FHIRPatient
	err = json.Unmarshal(v, &patient)
	is.NoErr(err)

	// Verify required fields
	is.True(patient.ID != "")
	is.True(len(patient.Name) > 0)
	is.True(len(patient.Name[0].Family) > 0)
	is.True(len(patient.Name[0].Given) > 0)
	is.True(patient.BirthDate != "")
	is.True(patient.Gender != "")
	is.True(len(patient.Address) > 0)
	is.True(len(patient.Address[0].Line) > 0)
	is.True(patient.Address[0].City != "")
	is.True(patient.Address[0].State != "")
	is.True(patient.Address[0].PostalCode != "")
	is.True(patient.Address[0].Country != "")
}

func TestSource_Read_HL7Data(t *testing.T) {
	is := is.New(t)
	underTest := openTestSource(
		t,
		map[string]string{
			"recordCount": "1",
			"format.type": "hl7",
			"operations":  "create",
		},
	)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)

	// Check that we got raw data (HL7 message)
	v, ok := rec.Payload.After.(opencdc.RawData)
	is.True(ok)

	// Convert to string and verify HL7 message structure
	message := string(v)

	// Check for required segments
	is.True(strings.HasPrefix(message, "MSH|"))
	is.True(strings.Contains(message, "\nPID|"))

	// Check for required fields in MSH segment
	mshFields := strings.Split(strings.Split(message, "\n")[0], "|")
	is.Equal(mshFields[3], "FACILITY") // Sending facility
	is.Equal(mshFields[5], "FACILITY") // Receiving facility
	is.Equal(mshFields[8], "ADT^A01")  // Message type
	is.Equal(mshFields[10], "P")       // Processing ID
	is.Equal(mshFields[11], "2.5")     // Version

	// Check for required fields in PID segment
	pidFields := strings.Split(strings.Split(message, "\n")[1], "|")
	is.Equal(pidFields[1], "1")                                 // Set ID
	is.True(pidFields[3] != "")                                 // Patient ID
	is.True(strings.Contains(pidFields[5], "^"))                // Patient name contains separator
	is.True(pidFields[7] != "")                                 // Birth date
	is.True(pidFields[8] == "male" || pidFields[8] == "female") // Gender
	is.True(strings.Contains(pidFields[11], "^"))               // Address contains separators
}

func openTestSource(t *testing.T, cfg map[string]string) sdk.Source {
	is := is.New(t)

	s := &Source{}
	t.Cleanup(func() {
		_ = s.Teardown(context.Background())
	})

	err := s.Configure(context.Background(), cfg)
	is.NoErr(err)

	err = s.Open(context.Background(), nil)
	is.NoErr(err)

	return s
}
