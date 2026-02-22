package main

import (
	"bufio"
	"flag"
	"fmt"
	"orrp-client/internal/client"
	"orrp-client/internal/tests"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7878", "Address of the orrp server")
	mode := flag.String("mode", "interactive", "Mode: 'interactive' (default) 'e2e', 'load', or 'bench'")

	suites := flag.String("suites", "all", "Comma-separated suites (ingest, query, pagination, robustness)")
	workers := flag.Int("workers", 20, "Load test concurrency")
	duration := flag.Int("duration", 5, "Load test duration (seconds)")

	flag.Parse()

	switch *mode {
	case "", "interactive":
		runInteractive(*addr)
	case "e2e", "load", "bench":
		cfg := tests.Config{
			Mode:         *mode,
			Address:      *addr,
			SuitesToRun:  strings.Split(*suites, ","),
			LoadWorkers:  *workers,
			LoadDuration: *duration,
		}
		tests.Run(cfg)
	default:
		fmt.Printf("❌ Invalid mode: %v\n", *mode)
		os.Exit(1)
	}
}

func runInteractive(addr string) {
	c, err := client.New(addr)
	if err != nil {
		fmt.Printf("❌ Error connecting: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("------------------------------------------------")
	fmt.Printf("Connected to orrp at %s\n", addr)
	fmt.Println("Type 'exit' to quit.")
	fmt.Println("------------------------------------------------")

	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)

		if text == "exit" || text == "quit" {
			break
		}
		if text == "" {
			continue
		}

		if err := c.SendCommand(text); err != nil {
			fmt.Printf("Send error: %v\n", err)
			break
		}

		response, err := c.ReadResponse()
		if err != nil {
			fmt.Printf("Read error: %v\n", err)
			break
		}

		fmt.Println(client.PrettyPrint(response))
	}
}
