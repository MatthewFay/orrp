package tests

import (
	"fmt"
	"orrp-e2e/client"
	"time"
)

type PaginationSuite struct{}

func (s *PaginationSuite) Name() string { return "pagination" }

func (s *PaginationSuite) Run(cfg Config) error {
	c, err := client.New(cfg.Address)
	if err != nil {
		return err
	}
	defer c.Close()

	ns := UniqueNamespace("page")

	testCases := []TestCase{
		{
			Name: "Cursor Pagination",
			Steps: []Step{
				{Command: fmt.Sprintf("EVENT in:%s entity:A eid:10 loc:ca", ns), Validator: ExpectOK},
				{Command: fmt.Sprintf("EVENT in:%s entity:B eid:20 loc:ca", ns), Validator: ExpectOK},
				{Command: fmt.Sprintf("EVENT in:%s entity:C eid:30 loc:ca", ns), Validator: ExpectOK},
				{Command: "SLEEP", Sleep: 50 * time.Millisecond},
				{
					Command:    fmt.Sprintf("QUERY in:%s where:(loc:ca)", ns),
					MaxRetries: 10,
					RetryDelay: 100 * time.Millisecond,
					Validator:  ExpectCount(3),
				},

				{
					Command:   fmt.Sprintf("QUERY in:%s where:(loc:ca) take:2", ns),
					Validator: ExpectCount(2),
				},
				{
					Command:   fmt.Sprintf("QUERY in:%s where:(loc:ca) take:3", ns),
					Validator: ExpectCount(3),
				},
				{
					Command:   fmt.Sprintf("QUERY in:%s where:(loc:ca) take:2", ns),
					Validator: ExpectNextCursor("3"),
				},

				{
					Command:   fmt.Sprintf("QUERY in:%s where:(loc:ca) cursor:3", ns),
					Validator: ExpectCount(1),
				},
			},
		},
	}

	return RunTestCases(c, testCases)
}
