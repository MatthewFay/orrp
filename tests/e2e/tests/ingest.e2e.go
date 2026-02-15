package tests

import (
	"fmt"
	"orrp-e2e/client"
)

type IngestSuite struct{}

func (s *IngestSuite) Name() string { return "ingest" }

func (s *IngestSuite) Run(cfg Config) error {
	c, err := client.New(cfg.Address)
	if err != nil { return err }
	defer c.Close()

	ns := UniqueNamespace("ingest")

	testCases := []TestCase{
		{
			Name: "Basic Ingestion",
			Steps: []Step{
				{
					Command:   fmt.Sprintf("EVENT in:%s entity:u1 loc:us type:login", ns),
					Validator: ExpectOK,
				},
				{
					Command:   fmt.Sprintf("EVENT in:%s entity:u2 loc:eu type:logout", ns),
					Validator: ExpectOK,
				},
			},
		},
	}

	return RunTestCases(c, testCases)
}
