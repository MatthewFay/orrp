package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7878", "Address of the orrp server")
	mode := flag.String("mode", "interactive", "Mode to run: 'interactive' or 'test'")
	flag.Parse()

	if *mode == "test" {
		RunE2ETests(*addr)
		return
	}

	// Interactive Mode
	runInteractive(*addr)
}

func runInteractive(addr string) {
	client, err := NewClient(addr)
	if err != nil {
		fmt.Printf("Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("------------------------------------------------")
	fmt.Printf("Connected to orrp at %s\n", addr)
	fmt.Println("Type commands (e.g. EVENT ... or QUERY ...)")
	fmt.Println("Type 'exit' or 'quit' to stop.")
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

		// 1. Send Command
		err := client.SendCommand(text)
		if err != nil {
			fmt.Printf("Error sending command: %v\n", err)
			// Try to reconnect? For now just exit loop
			break
		}

		// 2. Read MsgPack Response
		response, err := client.ReadResponse()
		if err != nil {
			fmt.Printf("Error reading response: %v\n", err)
			break
		}

		// 3. Print as JSON
		fmt.Println(PrettyPrint(response))
	}
}
