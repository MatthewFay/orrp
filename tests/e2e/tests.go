package main

import (
	"fmt"
	"log"
	"time"
)

// Step represents a single interaction with the DB
type Step struct {
	Command   string
	ExpectErr bool
	// Validator is a function you can write to inspect the generic response
	Validator func(response interface{}) error
}

type TestCase struct {
	Name  string
	Steps []Step
}

// RunE2ETests executes the defined test suite
func RunE2ETests(addr string) {
	log.Println("🚀 Starting E2E Test Suite...")

	// --- DEFINING THE TEST SUITE ---
	tests := []TestCase{
		{
			Name: "Smoke Test: Ingest and Query",
			Steps: []Step{
				{
					Command: "EVENT in:2025_09_01 entity:user123 loc:ca env:prod type:user.login",
					Validator: func(res interface{}) error {
						// Example: Check if server acknowledged the write (assuming it returns "OK" or similar map)
						// You can customize this based on your actual MsgPack schema
						return nil
					},
				},
				{
					Command: "EVENT in:2025_09_01 entity:user456 loc:ny env:prod type:user.logout",
				},
				{
					// Allow a tiny generic sleep to ensure consistency as db is eventually consistent
					Command: "SLEEP 100ms",
				},
				{
					Command: "QUERY in:2025_09_01 where:(loc:ca AND type:user.login)",
					Validator: func(res interface{}) error {
						// Example generic validation: assert we got a list back
						list, ok := res.([]interface{})
						if !ok {
							return fmt.Errorf("expected list response, got %T", res)
						}
						if len(list) == 0 {
							return fmt.Errorf("expected results, got empty list")
						}
						return nil
					},
				},
			},
		},
	}

	// --- EXECUTING THE SUITE ---
	client, err := NewClient(addr)
	if err != nil {
		log.Fatalf("❌ Fatal: Could not connect to DB for testing: %v", err)
	}
	defer client.Close()

	passed := 0
	failed := 0

	for _, test := range tests {
		fmt.Printf("Running: %s ... ", test.Name)
		err := runSingleTest(client, test)
		if err != nil {
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
		log.Fatal("Tests failed.")
	}
}

func runSingleTest(c *DBClient, t TestCase) error {
	for _, step := range t.Steps {
		// Simulation of test-side wait if needed
		if step.Command == "SLEEP 100ms" {
			time.Sleep(100 * time.Millisecond)
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
				return fmt.Errorf("validation failed on cmd '%s': %v\nResponse was: %s", step.Command, err, PrettyPrint(resp))
			}
		}
	}
	return nil
}
