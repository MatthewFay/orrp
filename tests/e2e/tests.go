package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// Step represents a single interaction with the DB
type Step struct {
	Command   string
	Validator func(response interface{}) error
}

type TestCase struct {
	Name  string
	Steps []Step
}

func RunE2ETests(addr string) {
	log.Println("🚀 Starting E2E Test Suite...")

	// Seed random for unique namespaces
	rand.Seed(time.Now().UnixNano())

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
				{Command: "SLEEP 200ms"},
				{
					// We query the specific namespace for this test run
					Command: fmt.Sprintf("QUERY in:%s where:(loc:ca)", nsMatch),
					Validator: func(res interface{}) error {
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
					Validator: func(res interface{}) error {
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
	for _, step := range t.Steps {
		if step.Command == "SLEEP 200ms" {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if err := c.SendCommand(step.Command); err != nil {
			return fmt.Errorf("send failed: %v", err)
		}

		resp, err := c.ReadResponse()
		if err != nil {
			return fmt.Errorf("read failed: %v", err)
		}

		if step.Validator != nil {
			if err := step.Validator(resp); err != nil {
				return err
			}
		}
	}
	return nil
}

func expectOK(res interface{}) error {
	m, ok := res.(map[string]interface{})
	if !ok || m["status"] != "OK" {
		return fmt.Errorf("expected status OK, got %v", res)
	}
	return nil
}

func extractObjects(res interface{}) ([]map[string]interface{}, error) {
	root, ok := res.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid root")
	}

	data, ok := root["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid data field")
	}

	list, ok := data["objects"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid objects field")
	}

	result := make([]map[string]interface{}, len(list))
	for i, v := range list {
		result[i] = v.(map[string]interface{})
	}
	return result, nil
}
