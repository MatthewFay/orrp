package tests

import (
	"fmt"
	"orrp-e2e/client"
	"strings"
	"time"
)

type Validator func(response any) error

type Step struct {
	Command    string
	MaxRetries int
	RetryDelay time.Duration
	Validator  Validator
	Sleep      time.Duration
}

type TestCase struct {
	Name  string
	Steps []Step
}

func RunTestCases(c *client.DBClient, tests []TestCase) error {
	for _, t := range tests {
		fmt.Printf("   ├─ %-40s ", t.Name+"...")
		if err := runSingleTest(c, t); err != nil {
			fmt.Printf("FAILED ❌\n")
			return fmt.Errorf("test '%s' failed: %v", t.Name, err)
		}
		fmt.Printf("PASSED ✅\n")
	}
	return nil
}

func runSingleTest(c *client.DBClient, t TestCase) error {
	for i, step := range t.Steps {
		if step.Sleep > 0 {
			time.Sleep(step.Sleep)
		}
		
		if after, ok := strings.CutPrefix(step.Command, "SLEEP "); ok {
			d, _ := time.ParseDuration(after)
			time.Sleep(d)
			continue
		}

		attempts := 1 + step.MaxRetries
		var lastErr error

		for attempt := 1; attempt <= attempts; attempt++ {
			lastErr = nil

			if err := c.SendCommand(step.Command); err != nil {
				lastErr = fmt.Errorf("send failed: %v", err)
				break 
			}

			resp, err := c.ReadResponse()
			if err != nil {
				lastErr = fmt.Errorf("read failed: %v", err)
				break
			}

			if step.Validator != nil {
				if err := step.Validator(resp); err != nil {
					lastErr = err
				}
			}

			if lastErr == nil {
				break
			}

			if attempt < attempts {
				time.Sleep(step.RetryDelay)
			}
		}

		if lastErr != nil {
			return fmt.Errorf("step %d failed: %v", i+1, lastErr)
		}
	}
	return nil
}
