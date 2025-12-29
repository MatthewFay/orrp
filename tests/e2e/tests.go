package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Step represents a single interaction with the DB
type Step struct {
	Command string
	// Optional: How many times to retry this step if Validator fails?
	// Useful for waiting on eventual consistency (queries).
	MaxRetries int
	// Optional: How long to wait between retries
	RetryDelay time.Duration

	Validator func(response any) error
}

type TestCase struct {
	Name  string
	Steps []Step
}

func RunE2ETests(addr string) {
	log.Println("🚀 Starting E2E Test Suite...")

	// We generate namespaces dynamically so tests never collide with previous runs
	nsStatus := uniqueNamespace("status")
	nsMatch := uniqueNamespace("match")
	nsNoMatch := uniqueNamespace("nomatch")

	tests := []TestCase{
		{
			Name: "Ingest and Verify Status",
			Steps: []Step{
				{
					Command:   fmt.Sprintf("EVENT in:%s entity:user100 loc:ca type:user.login", nsStatus),
					Validator: expectOK,
				},
			},
		},
		{
			Name: "Query with Filtering (Match)",
			Steps: []Step{
				{
					Command:   fmt.Sprintf("EVENT in:%s entity:user101 loc:ca type:user.login", nsMatch),
					Validator: expectOK,
				},
				{
					Command:   fmt.Sprintf("EVENT in:%s entity:user102 loc:ny type:user.login", nsMatch),
					Validator: expectOK,
				},
				// We keep the SLEEP for basic buffering, but rely on retries for the check
				{Command: "SLEEP 50ms"},
				{
					Command:    fmt.Sprintf("QUERY in:%s where:(loc:ca)", nsMatch),
					MaxRetries: 5,
					RetryDelay: 100 * time.Millisecond,
					Validator: func(res any) error {
						objects, err := extractObjects(res)
						if err != nil {
							return err
						}
						if len(objects) != 1 {
							return fmt.Errorf("expected 1 object, got %d", len(objects))
						}
						if objects[0]["entity"] != "user101" {
							return fmt.Errorf("expected entity 'user101', got '%v'", objects[0]["entity"])
						}
						return nil
					},
				},
			},
		},
		{
			Name: "Query with Filtering (No Match)",
			Steps: []Step{
				{
					Command:   fmt.Sprintf("EVENT in:%s entity:user200 loc:tx type:user.purchase", nsNoMatch),
					Validator: expectOK,
				},
				{Command: "SLEEP 200ms"},
				{
					Command: fmt.Sprintf("QUERY in:%s where:(loc:fl)", nsNoMatch),
					Validator: func(res any) error {
						objects, err := extractObjects(res)
						if err != nil {
							return err
						}
						if len(objects) != 0 {
							return fmt.Errorf("expected 0 objects, got %d", len(objects))
						}
						return nil
					},
				},
			},
		},
		{
			Name: "Query numeric entity",
			Steps: []Step{
				{
					Command:   fmt.Sprintf("EVENT in:%s entity:100 loc:CA type:user.purchase test:numeric_entity", nsMatch),
					Validator: expectOK,
				},
				{Command: "SLEEP 50ms"},
				{
					Command:    fmt.Sprintf("QUERY in:%s where:(test:numeric_entity)", nsMatch),
					MaxRetries: 5,
					RetryDelay: 100 * time.Millisecond,
					Validator: func(res any) error {
						objects, err := extractObjects(res)
						if err != nil {
							return err
						}
						if len(objects) != 1 {
							return fmt.Errorf("expected 1 object, got %d", len(objects))
						}
						ent, _ := asInt64(objects[0]["entity"])
						if ent != 100 {
							return fmt.Errorf("expected entity 100, got '%v'", objects[0]["entity"])
						}
						return nil
					},
				},
			},
		},
	}

	// --- EXECUTION ---
	client, err := NewClient(addr)
	if err != nil {
		log.Fatalf("❌ Fatal: Could not connect to DB: %v", err)
	}
	defer client.Close()

	passed, failed := 0, 0
	for _, test := range tests {
		fmt.Printf("Running: %-40s ", test.Name+"...")
		if err := runSingleTest(client, test); err != nil {
			fmt.Printf("FAILED ❌\n   Reason: %v\n", err)
			failed++
		} else {
			fmt.Printf("PASSED ✅\n")
			passed++
		}
	}

	fmt.Println("------------------------------------------------")
	fmt.Printf("Test Summary: %d Passed, %d Failed\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

// --- HELPERS ---

// uniqueNamespace creates a string like "test_match_174123912"
func uniqueNamespace(prefix string) string {
	return fmt.Sprintf("test_%s_%d_%d", prefix, time.Now().UnixNano(), rand.Intn(1000))
}

func runSingleTest(c *DBClient, t TestCase) error {
	for i, step := range t.Steps {
		if after, ok := strings.CutPrefix(step.Command, "SLEEP "); ok {
			durationStr := after
			d, err := time.ParseDuration(durationStr)
			if err == nil {
				time.Sleep(d)
				continue
			}
		}

		attempts := 1 + step.MaxRetries
		var lastErr error

		for attempt := 1; attempt <= attempts; attempt++ {
			// Clear error from previous attempt
			lastErr = nil

			if err := c.SendCommand(step.Command); err != nil {
				lastErr = fmt.Errorf("send failed: %v", err)
				// If send fails (network error), we don't retry
				break
			}

			resp, err := c.ReadResponse()
			if err != nil {
				lastErr = fmt.Errorf("read failed: %v", err)
				break
			}

			if step.Validator != nil {
				if err := step.Validator(resp); err != nil {
					lastErr = err // Capture validation error
				}
			}

			if lastErr == nil {
				break
			}

			if attempt < attempts {
				// fmt.Printf("(retrying step %d...)", i) // Optional debug log
				time.Sleep(step.RetryDelay)
			}
		}

		if lastErr != nil {
			return fmt.Errorf("step %d failed after %d attempts: %v", i+1, attempts, lastErr)
		}
	}
	return nil
}

func expectOK(res any) error {
	m, ok := res.(map[string]any)
	if !ok || m["status"] != "OK" {
		return fmt.Errorf("expected status OK, got %v", res)
	}
	return nil
}

func extractObjects(res any) ([]map[string]any, error) {
	root, ok := res.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid root")
	}

	data, ok := root["data"].(map[string]any)
	if !ok {
		// It's possible "data" is missing if status != OK, handled by callers usually
		return nil, fmt.Errorf("invalid data field or missing in response")
	}

	list, ok := data["objects"].([]any)
	if !ok {
		return nil, fmt.Errorf("invalid objects field")
	}

	result := make([]map[string]any, len(list))
	for i, v := range list {
		result[i] = v.(map[string]any)
	}
	return result, nil
}

func asInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case uint:
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("unsupported type: %T", v)
	}
}
