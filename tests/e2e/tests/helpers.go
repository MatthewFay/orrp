package tests

import (
	"fmt"
	"math/rand"
	"time"
)

func UniqueNamespace(prefix string) string {
	return fmt.Sprintf("test_%s_%d_%d", prefix, time.Now().UnixNano(), rand.Intn(1000))
}

func ExtractObjects(res any) ([]map[string]any, error) {
	m, ok := res.(map[string]any)
	if !ok { return nil, fmt.Errorf("resp not a map") }
	
	if m["data"] == nil { return nil, fmt.Errorf("data is nil") }
	data, ok := m["data"].(map[string]any)
	if !ok { return nil, fmt.Errorf("data malformed") }

	list, ok := data["objects"].([]any)
	if !ok { return nil, fmt.Errorf("objects missing/malformed") }

	results := make([]map[string]any, len(list))
	for i, v := range list {
		results[i] = v.(map[string]any)
	}
	return results, nil
}
