package tests

import (
	"fmt"
)

func ExpectOK(res any) error {
	m, ok := res.(map[string]any)
	if !ok || m["status"] != "OK" {
		return fmt.Errorf("expected status OK, got %v", res)
	}
	return nil
}

func ExpectError(res any) error {
	m, ok := res.(map[string]any)
	if !ok || m["status"] == "OK" {
		return fmt.Errorf("expected error, got OK")
	}
	return nil
}

func ExpectCount(n int) Validator {
	return func(res any) error {
		objs, err := ExtractObjects(res)
		if err != nil {
			return err
		}
		if len(objs) != n {
			return fmt.Errorf("expected %d objects, got %d", n, len(objs))
		}
		return nil
	}
}

func ExpectEntity(val string) Validator {
	return func(res any) error {
		objs, err := ExtractObjects(res)
		if err != nil { return err }
		if len(objs) == 0 { return fmt.Errorf("no objects returned") }
		
		got := fmt.Sprintf("%v", objs[0]["entity"])
		if got != val {
			return fmt.Errorf("expected entity '%s', got '%s'", val, got)
		}
		return nil
	}
}

func ExpectNextCursor(val string) Validator {
	return func(res any) error {
		m, ok := res.(map[string]any)
		if !ok { return fmt.Errorf("invalid response") }
		data, ok := m["data"].(map[string]any)
		if !ok { return fmt.Errorf("invalid data") }
		
		got := fmt.Sprintf("%v", data["next_cursor"])
		if got != val {
			return fmt.Errorf("expected next_cursor '%s', got '%s'", val, got)
		}
		return nil
	}
}
