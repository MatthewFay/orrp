package tests

import (
	"orrp-client/internal/client"
)

type RobustnessSuite struct{}

func (s *RobustnessSuite) Name() string { return "robustness" }

func (s *RobustnessSuite) Run(cfg Config) error {
	c, err := client.New(cfg.Address)
	if err != nil {
		return err
	}
	defer c.Close()

	testCases := []TestCase{
		{
			Name: "Invalid Commands",
			Steps: []Step{
				{
					Command:   "GARBAGE_COMMAND args:none",
					Validator: ExpectError,
				},
				{
					Command:   "EVENT missing_args",
					Validator: ExpectError,
				},
			},
		},
		{
			Name: "Server Alive Check",
			Steps: []Step{
				{
					Command:   "EVENT in:robust entity:alive type:check",
					Validator: ExpectOK,
				},
			},
		},
	}

	return RunTestCases(c, testCases)
}
