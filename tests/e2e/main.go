package main

import (
	"bufio"
	"flag"
	"fmt"
	"orrp-e2e/client"
	"orrp-e2e/tests"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7878", "Address of the orrp server")
	mode := flag.String("mode", "interactive", "Mode: 'interactive' (default) 'e2e', or 'load'")

	suites := flag.String("suites", "all", "Comma-separated suites (ingest, query, pagination, robustness)")
	workers := flag.Int("workers", 20, "Load test concurrency")
	duration := flag.Int("duration", 5, "Load test duration (seconds)")

	flag.Parse()

	if *mode == "e2e" || *mode == "load" {
		cfg := tests.Config{
			Mode:         *mode,
			Address:      *addr,
			SuitesToRun:  strings.Split(*suites, ","),
			LoadWorkers:  *workers,
			LoadDuration: *duration,
		}
		tests.Run(cfg)
		return
	}

	runInteractive(*addr)
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
