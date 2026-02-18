package tests

import (
	"fmt"
	"math/rand"
	"orrp-e2e/client"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

type LoadPerformanceSuite struct{}

func (s *LoadPerformanceSuite) Name() string { return "load" }

func (s *LoadPerformanceSuite) Run(cfg Config) error {
	ns := UniqueNamespace("bench")
	var results []*PhaseResult

	// Shared Random Data
	locations := []string{"aws-us-east", "aws-us-west", "gcp-eu-west", "azure-asia"}
	eventTypes := []string{"click", "view", "purchase", "login", "logout"}

	getRealisticUser := func(r *rand.Rand) string {
		if r.Float32() < 0.8 {
			return fmt.Sprintf("user_%d", r.Intn(1000))
		}
		return fmt.Sprintf("user_%d", 1000+r.Intn(999000))
	}

	getWeightedItem := func(r *rand.Rand, items []string) string {
		return items[r.Intn(len(items))]
	}

	// ========================================================================
	// PHASE 1: INGEST & SETTLE
	// ========================================================================
	fmt.Printf("\n   Phase 1: High Velocity Ingestion (%d workers)...\n", cfg.LoadWorkers)

	writeGen := func(r *rand.Rand, workerID int) string {
		user := getRealisticUser(r)
		loc := getWeightedItem(r, locations)
		evt := getWeightedItem(r, eventTypes)
		return fmt.Sprintf("EVENT in:%s entity:%s loc:%s type:%s meta:session_%d", ns, user, loc, evt, r.Intn(99999))
	}

	writeVal := func(res any) error { return ExpectOK(res) }

	writeStats := runLoadPhase(cfg, "persistent", writeGen, writeVal)
	if err := checkHealth("Ingest", writeStats, false); err != nil {
		return err
	}

	fmt.Print("      Measuring Index Lag (Settling Time)... ")
	settleTime, err := measureSettlingTime(cfg, ns)
	if err != nil {
		fmt.Printf("FAILED (%v)\n", err)
		return err
	}
	fmt.Printf("%v\n", settleTime)
	results = append(results, summarize("Ingest", writeStats, cfg.LoadDuration, settleTime))

	// ========================================================================
	// PHASE 2: QUERY
	// ========================================================================
	fmt.Printf("\n   Phase 2: High Velocity Querying (%d workers)...\n", cfg.LoadWorkers)

	readGen := func(r *rand.Rand, workerID int) string {
		if r.Float32() < 0.05 {
			return fmt.Sprintf("QUERY in:%s where:(loc:non_existent_zone)", ns)
		}
		loc := getWeightedItem(r, locations)
		return fmt.Sprintf("QUERY in:%s where:(loc:%s) take:5", ns, loc)
	}

	readVal := func(res any) error {
		_, err := ExtractObjects(res)
		return err
	}

	readStats := runLoadPhase(cfg, "persistent", readGen, readVal)
	if err := checkHealth("Query", readStats, false); err != nil {
		return err
	}
	results = append(results, summarize("Query", readStats, cfg.LoadDuration, 0))

	// ========================================================================
	// PHASE 3: MIXED
	// ========================================================================
	fmt.Printf("\n   Phase 3: Mixed Workload (50%% Read / 50%% Write)...\n")

	mixedGen := func(r *rand.Rand, workerID int) string {
		if r.Float32() < 0.5 {
			return writeGen(r, workerID)
		}
		return readGen(r, workerID)
	}

	mixedStats := runLoadPhase(cfg, "persistent", mixedGen, nil)
	if err := checkHealth("Mixed", mixedStats, false); err != nil {
		return err
	}
	results = append(results, summarize("Mixed", mixedStats, cfg.LoadDuration, 0))

	// ========================================================================
	// PHASE 4: COMPLEX PAYLOAD (Heavy Tags)
	// ========================================================================
	fmt.Printf("\n   Phase 4: Complex Payload (Many Tags, < 2KB)...\n")

	complexGen := func(r *rand.Rand, workerID int) string {
		sb := strings.Builder{}
		sb.WriteString(fmt.Sprintf("EVENT in:%s entity:complex_%d type:heavy ", ns, r.Intn(1000)))

		// Add 15 random tags to stress the Indexer
		for i := 0; i < 15; i++ {
			val := fmt.Sprintf("val_%d_%s", r.Intn(100), "abcdefgh")
			sb.WriteString(fmt.Sprintf("tag_%d:%s ", i, val))
		}
		return sb.String()
	}

	complexStats := runLoadPhase(cfg, "persistent", complexGen, writeVal)
	if err := checkHealth("Complex", complexStats, false); err != nil {
		return err
	}
	results = append(results, summarize("Complex", complexStats, cfg.LoadDuration, 0))

	// ========================================================================
	// PHASE 5: CONNECTION STORM
	// ========================================================================
	fmt.Printf("\n   Phase 5: Connection Storm (Connect -> Query -> Close)...\n")

	connStormGen := func(r *rand.Rand, workerID int) string {
		return fmt.Sprintf("QUERY in:%s where:(loc:aws-us-east) take:1", ns)
	}

	stormStats := runLoadPhase(cfg, "churn", connStormGen, nil)

	if err := checkHealth("ConnStorm", stormStats, true); err != nil {
		if strings.Contains(err.Error(), "0 successful") {
			return err
		}
		fmt.Printf("      (Note: Server actively failed connections: %v)\n", err)
	}

	// Report on saturation for user awareness
	var clientSat int64
	for _, w := range stormStats {
		clientSat += w.ClientLimitCount
	}
	if clientSat > 0 {
		fmt.Printf("      ! Test Setup Warning: Client exhausted local ports %d times.\n", clientSat)
		fmt.Printf("      ! (This is a client-side limit, not a DB failure)\n")
	}

	results = append(results, summarize("ConnStorm", stormStats, cfg.LoadDuration, 0))

	// ========================================================================
	// FINAL REPORT
	// ========================================================================
	printReport(cfg, results)

	return nil
}

// ----------------------------------------------------------------------------
// TEST HELPERS
// ----------------------------------------------------------------------------

func checkHealth(phase string, workers []*WorkerStats, ignoreClientLimits bool) error {
	var totalReqs, totalErrs int64
	for _, w := range workers {
		totalReqs += w.SuccessCount
		totalErrs += w.ErrorCount // Server errors only
	}

	if totalReqs == 0 {
		return fmt.Errorf("FAILURE: %s phase produced 0 successful requests (System Down?)", phase)
	}

	totalOps := totalReqs + totalErrs
	if totalOps == 0 {
		return nil
	}

	errorRate := float64(totalErrs) / float64(totalOps)

	if errorRate > 0.01 {
		return fmt.Errorf("FAILURE: %s phase Server Error rate %.2f%% exceeded limit (1%%)", phase, errorRate*100)
	}
	return nil
}

func measureSettlingTime(cfg Config, ns string) (time.Duration, error) {
	c, err := client.New(cfg.Address)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	probeID := fmt.Sprintf("probe_%d", time.Now().UnixNano())
	start := time.Now()

	if err := c.SendCommand(fmt.Sprintf("EVENT in:%s entity:%s loc:probe type:probe", ns, probeID)); err != nil {
		return 0, err
	}
	if _, err := c.ReadResponse(); err != nil {
		return 0, err
	}

	attempts := 12000 // 2 mins
	for i := 0; i < attempts; i++ {
		c.SendCommand(fmt.Sprintf("QUERY in:%s where:(type:probe)", ns))
		res, err := c.ReadResponse()
		if err == nil {
			if objs, _ := ExtractObjects(res); len(objs) > 0 {
				return time.Since(start), nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("timeout waiting for indexer > %d attempts", attempts)
}

// ----------------------------------------------------------------------------
// LOAD GENERATOR ENGINE
// ----------------------------------------------------------------------------

type WorkerStats struct {
	ID               int
	SuccessCount     int64
	ErrorCount       int64
	ClientLimitCount int64
	Latencies        []time.Duration
}

const MaxLatencySamples = 10000

func runLoadPhase(cfg Config,
	mode string,
	cmdGenerator func(*rand.Rand, int) string,
	validator func(any) error,
) []*WorkerStats {

	allWorkerStats := make([]*WorkerStats, cfg.LoadWorkers)
	for i := 0; i < cfg.LoadWorkers; i++ {
		allWorkerStats[i] = &WorkerStats{
			ID:        i,
			Latencies: make([]time.Duration, 0, MaxLatencySamples),
		}
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	for i := 0; i < cfg.LoadWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			myStats := allWorkerStats[id]
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			var c *client.DBClient
			var err error

			if mode == "persistent" {
				c, err = client.New(cfg.Address)
				if err != nil {
					fmt.Printf("Worker %d init failed: %v\n", id, err)
					myStats.ErrorCount++
					return
				}
				defer c.Close()
			}

			for {
				select {
				case <-stopCh:
					return
				default:
					cmd := cmdGenerator(r, id)
					start := time.Now()

					if mode == "churn" {
						c, err = client.New(cfg.Address)
						if err != nil {
							msg := err.Error()
							if strings.Contains(msg, "assign requested address") ||
								strings.Contains(msg, "no buffer space") {
								myStats.ClientLimitCount++
							} else {
								myStats.ErrorCount++
							}
							continue
						}
					}

					if err := c.SendCommand(cmd); err != nil {
						myStats.ErrorCount++
						if mode == "churn" {
							c.Close()
						}
						continue
					}

					resp, err := c.ReadResponse()
					dur := time.Since(start)

					if mode == "churn" {
						c.Close()
					}

					if err != nil {
						myStats.ErrorCount++
						continue
					}

					if validator != nil {
						if err := validator(resp); err != nil {
							myStats.ErrorCount++
							continue
						}
					}

					myStats.SuccessCount++
					if len(myStats.Latencies) < MaxLatencySamples {
						myStats.Latencies = append(myStats.Latencies, dur)
					} else {
						idx := r.Intn(int(myStats.SuccessCount))
						if idx < MaxLatencySamples {
							myStats.Latencies[idx] = dur
						}
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(cfg.LoadDuration) * time.Second)
	close(stopCh)
	wg.Wait()

	return allWorkerStats
}

// ----------------------------------------------------------------------------
// REPORTING
// ----------------------------------------------------------------------------

type PhaseResult struct {
	Name         string
	Duration     int
	Aggregate    AggStats
	Workers      []*WorkerStats
	SettlingTime time.Duration
}

type AggStats struct {
	TotalOK       int64
	TotalErr      int64
	TotalLimitErr int64
	TotalRPS      float64
	P50           time.Duration
	P90           time.Duration
	P95           time.Duration
	Max           time.Duration
}

func summarize(name string, workers []*WorkerStats, durationSec int, settle time.Duration) *PhaseResult {
	res := &PhaseResult{
		Name:         name,
		Duration:     durationSec,
		Workers:      workers,
		SettlingTime: settle,
	}

	var allLatencies []time.Duration
	for _, w := range workers {
		res.Aggregate.TotalOK += w.SuccessCount
		res.Aggregate.TotalErr += w.ErrorCount
		res.Aggregate.TotalLimitErr += w.ClientLimitCount
		allLatencies = append(allLatencies, w.Latencies...)
	}

	res.Aggregate.TotalRPS = float64(res.Aggregate.TotalOK) / float64(durationSec)

	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })

	getPerc := func(p float64) time.Duration {
		if len(allLatencies) == 0 {
			return 0
		}
		return allLatencies[int(float64(len(allLatencies))*p)]
	}

	if len(allLatencies) > 0 {
		res.Aggregate.P50 = getPerc(0.50)
		res.Aggregate.P90 = getPerc(0.90)
		res.Aggregate.P95 = getPerc(0.95)
		res.Aggregate.Max = allLatencies[len(allLatencies)-1]
	}

	return res
}

func printReport(cfg Config, results []*PhaseResult) {
	fmt.Println("\n======================================================================")
	fmt.Println("  ORRP BENCHMARK REPORT")
	fmt.Printf("  Config: %d Workers | %ds | %s\n", cfg.LoadWorkers, cfg.LoadDuration, cfg.Address)
	fmt.Println("======================================================================")

	// --- TRAFFIC SUMMARY ---
	fmt.Println("\n[ TRAFFIC SUMMARY ]")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "PHASE\tOK\tRPS\tSVR ERR\tCLI LIM\tLAG\t")
	fmt.Fprintln(w, "-----\t--\t---\t----\t----\t---\t")

	for _, r := range results {
		settleStr := "-"
		if r.SettlingTime > 0 {
			settleStr = r.SettlingTime.String()
		}
		fmt.Fprintf(w, "%s\t%d\t%.0f\t%d\t%d\t%s\t\n",
			r.Name, r.Aggregate.TotalOK, r.Aggregate.TotalRPS,
			r.Aggregate.TotalErr, r.Aggregate.TotalLimitErr, settleStr)
	}
	w.Flush()

	// --- AGGREGATE LATENCY ---
	fmt.Println("\n[ LATENCY STATS ]")
	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "PHASE\tP50\tP90\tP95\tMAX\t")
	fmt.Fprintln(w, "-----\t---\t---\t---\t---\t")
	for _, r := range results {
		fmt.Fprintf(w, "%s\t%v\t%v\t%v\t%v\t\n",
			r.Name, r.Aggregate.P50, r.Aggregate.P90, r.Aggregate.P95, r.Aggregate.Max)
	}
	w.Flush()

	// --- DETAILED WORKER BREAKDOWN ---
	fmt.Println("\n----------------------------------------------------------------------")
	fmt.Println("  DETAILED WORKER BREAKDOWN")
	fmt.Println("----------------------------------------------------------------------")

	for _, r := range results {
		fmt.Printf("\n[ %s ]\n", r.Name)
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight|tabwriter.Debug)
		// ADDED P50 and P90 back to this header
		fmt.Fprintln(w, "ID\tOK\tP50\tP90\tP95\tMAX\tERRS\t")
		fmt.Fprintln(w, "--\t--\t---\t---\t---\t---\t----\t")

		for _, worker := range r.Workers {
			p50, p90, p95, maxLat := time.Duration(0), time.Duration(0), time.Duration(0), time.Duration(0)

			if len(worker.Latencies) > 0 {
				lats := make([]time.Duration, len(worker.Latencies))
				copy(lats, worker.Latencies)
				sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

				count := float64(len(lats))
				p50 = lats[int(count*0.50)]
				p90 = lats[int(count*0.90)]
				p95 = lats[int(count*0.95)]
				maxLat = lats[len(lats)-1]
			}

			totalErr := worker.ErrorCount + worker.ClientLimitCount

			fmt.Fprintf(w, "%d\t%d\t%v\t%v\t%v\t%v\t%d\t\n",
				worker.ID, worker.SuccessCount, p50, p90, p95, maxLat, totalErr)
		}
		w.Flush()
	}
	fmt.Println("======================================================================")
}
