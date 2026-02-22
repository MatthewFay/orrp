package tests

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	Mode         string
	Address      string
	SuitesToRun  []string
	LoadWorkers  int
	LoadDuration int
}

type Suite interface {
	Name() string
	Run(cfg Config) error
}

func Run(cfg Config) {
	fmt.Printf("🚀 Starting Orrp %s Suite...\n", strings.ToUpper(cfg.Mode))
	fmt.Printf("   Target: %s\n", cfg.Address)

	allSuites := []Suite{}
	switch cfg.Mode {
	case "e2e":
		allSuites = []Suite{
			&IngestSuite{},
			&QuerySuite{},
			&PaginationSuite{},
			&RobustnessSuite{},
		}
	case "load":
		allSuites = []Suite{
			&LoadPerformanceSuite{},
		}
	case "bench":
		allSuites = []Suite{
			&BenchmarkSuite{},
		}
	}

	passed, failed := 0, 0

	for _, suite := range allSuites {
		if shouldRun(suite.Name(), cfg.SuitesToRun) {
			fmt.Println("------------------------------------------------")
			fmt.Printf("📦 Running Suite: %s\n", suite.Name())

			start := time.Now()
			if err := suite.Run(cfg); err != nil {
				fmt.Printf("❌ SUITE FAILED: %v\n", err)
				failed++
			} else {
				fmt.Printf("✅ SUITE PASSED (%v)\n", time.Since(start).Round(time.Millisecond))
				passed++
			}
		}
	}

	fmt.Println("------------------------------------------------")
	fmt.Printf("SUMMARY: %d Passed, %d Failed\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func shouldRun(name string, requested []string) bool {
	if len(requested) == 0 || requested[0] == "all" || requested[0] == "" {
		return true
	}
	for _, req := range requested {
		if strings.Contains(strings.ToLower(name), strings.ToLower(strings.TrimSpace(req))) {
			return true
		}
	}
	return false
}
