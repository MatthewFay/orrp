package tests

import (
	"fmt"
	"orrp-client/internal/client"
	"time"
)

type QuerySuite struct{}

func (s *QuerySuite) Name() string { return "query" }

func (s *QuerySuite) Run(cfg Config) error {
	c, err := client.New(cfg.Address)
	if err != nil {
		return err
	}
	defer c.Close()

	ns := UniqueNamespace("query")

	testCases := []TestCase{
		{
			Name: "Query with Filtering",
			Steps: []Step{
				{Command: fmt.Sprintf("EVENT in:%s entity:u1 loc:ca type:login", ns), Validator: ExpectOK},
				{Command: fmt.Sprintf("EVENT in:%s entity:u2 loc:ny type:login", ns), Validator: ExpectOK},
				{Command: "SLEEP", Sleep: 250 * time.Millisecond},
				{
					Command:    fmt.Sprintf("QUERY in:%s where:(loc:ca)", ns),
					MaxRetries: 10,
					RetryDelay: 100 * time.Millisecond,
					Validator:  ExpectCount(1),
				},
				{
					Command:   fmt.Sprintf("QUERY in:%s where:(loc:ca)", ns),
					Validator: ExpectEntity("u1"),
				},
			},
		},
		{
			Name: "Query No Match",
			Steps: []Step{
				{
					Command:    fmt.Sprintf("QUERY in:%s where:(loc:texas)", ns),
					MaxRetries: 5,
					RetryDelay: 100 * time.Millisecond,
					Validator:  ExpectCount(0),
				},
			},
		},
	}

	return RunTestCases(c, testCases)
}
