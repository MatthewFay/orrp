package tests

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"math/rand"
	"orrp-client/internal/client"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

//go:embed bench_template.html
var benchHTMLTemplate string

type BenchmarkSuite struct{}

func (s *BenchmarkSuite) Name() string { return "bench" }

// --- Standalone Bench Data Structures ---

type BenchWorkerStats struct {
	ID           int
	SuccessCount int64
	ErrorCount   int64
	Latencies    []time.Duration
}

type BenchWorkerRow struct {
	ID           int     `json:"id"`
	SuccessCount int64   `json:"ok"`
	ErrorCount   int64   `json:"err"`
	P50Ms        float64 `json:"p50"`
	P90Ms        float64 `json:"p90"`
	P95Ms        float64 `json:"p95"`
	P99Ms        float64 `json:"p99"`
	MaxMs        float64 `json:"max"`
}

type BenchAggStats struct {
	TotalOK  int64   `json:"TotalOK"`
	TotalErr int64   `json:"TotalErr"`
	TotalRPS float64 `json:"TotalRPS"`
	P50Ms    float64 `json:"P50Ms"`
	P90Ms    float64 `json:"P90Ms"`
	P95Ms    float64 `json:"P95Ms"`
	P99Ms    float64 `json:"P99Ms"`
	MaxMs    float64 `json:"MaxMs"`
}

type BenchPhaseResult struct {
	Name      string           `json:"Name"`
	Duration  int              `json:"Duration"`
	Aggregate BenchAggStats    `json:"Aggregate"`
	LagMs     float64          `json:"LagMs"`
	Workers   []BenchWorkerRow `json:"Workers"`
}

type HardwareInfo struct {
	OS           string
	Arch         string
	LogicalCPUs  int
	PhysicalCPUs int
	CPUName      string
	RAMTotalGB   float64
}

type BenchReportData struct {
	Timestamp   string
	Workers     int
	DurationSec int
	Hardware    HardwareInfo
	Results     []*BenchPhaseResult
	ResultsJSON template.JS
}

const BenchMaxLatencySamples = 20000

func (s *BenchmarkSuite) Run(cfg Config) error {
	fmt.Println("\n======================================================================")
	fmt.Println("  ORRP STANDARDIZED BENCHMARK SUITE")
	fmt.Printf("  Config: %d Workers | %ds per phase\n", cfg.LoadWorkers, cfg.LoadDuration)
	fmt.Println("======================================================================")

	hw := getHardwareInfo()
	fmt.Printf("💻 Detected Hardware: %s (%d Physical / %d Logical Cores) | %.1f GB RAM\n",
		hw.CPUName, hw.PhysicalCPUs, hw.LogicalCPUs, hw.RAMTotalGB)

	ns := UniqueNamespace("bench_v1")
	var results []*BenchPhaseResult

	locations := []string{"aws-us-east", "aws-us-west", "gcp-eu-west"}
	eventTypes := []string{"click", "view", "purchase", "login", "logout"}

	getRealisticUser := func(r *rand.Rand) string {
		if r.Float32() < 0.8 {
			return fmt.Sprintf("user_%d", r.Intn(1000))
		}
		return fmt.Sprintf("user_%d", 1000+r.Intn(999000))
	}

	// ------------------------------------------------------------------------
	// V1: 100% Ingestion (Write Heavy)
	// ------------------------------------------------------------------------
	fmt.Printf("\n▶ Running [v1_100%%_Ingest] (100%% Writes)...\n")
	writeGen := func(r *rand.Rand, workerID int) string {
		user := getRealisticUser(r)
		loc := locations[r.Intn(len(locations))]
		evt := eventTypes[r.Intn(len(eventTypes))]
		return fmt.Sprintf("EVENT in:%s entity:%s loc:%s type:%s meta:session_%d", ns, user, loc, evt, r.Intn(99999))
	}

	writeStats := runBenchPhase(cfg, writeGen, ExpectOK)

	fmt.Print("  Measuring Index Lag (Settling Time)... ")
	settleTime, err := measureBenchLag(cfg, ns)
	if err != nil {
		fmt.Printf("FAILED (%v)\n", err)
	} else {
		fmt.Printf("%v\n", settleTime)
	}
	results = append(results, summarizeBench("v1_100%_Ingest", writeStats, cfg.LoadDuration, settleTime))

	// ------------------------------------------------------------------------
	// V1: 100% Query (Read Heavy)
	// ------------------------------------------------------------------------
	fmt.Printf("\n▶ Running [v1_100%%_Query] (100%% Reads)...\n")
	readGen := func(r *rand.Rand, workerID int) string {
		loc := locations[r.Intn(len(locations))]
		return fmt.Sprintf("QUERY in:%s where:(loc:%s) take:10", ns, loc)
	}

	readStats := runBenchPhase(cfg, readGen, nil)
	results = append(results, summarizeBench("v1_100%_Query", readStats, cfg.LoadDuration, 0))

	// ------------------------------------------------------------------------
	// V1: Mixed (50% Read / 50% Write)
	// ------------------------------------------------------------------------
	fmt.Printf("\n▶ Running [v1_50%%_Mixed] (50%% Read / 50%% Write)...\n")
	mixedGen := func(r *rand.Rand, workerID int) string {
		if r.Float32() < 0.5 {
			return writeGen(r, workerID)
		}
		return readGen(r, workerID)
	}

	mixedStats := runBenchPhase(cfg, mixedGen, nil)

	fmt.Print("  Measuring Index Lag (Settling Time)... ")
	mixedSettle, _ := measureBenchLag(cfg, ns)
	fmt.Printf("%v\n", mixedSettle)

	results = append(results, summarizeBench("v1_50%_Mixed", mixedStats, cfg.LoadDuration, mixedSettle))

	// ------------------------------------------------------------------------
	// V1: Complex Payload (Heavy Tags)
	// ------------------------------------------------------------------------
	fmt.Printf("\n▶ Running [v1_Complex_Payload] (Many Tags, < 2KB)...\n")
	complexGen := func(r *rand.Rand, workerID int) string {
		sb := strings.Builder{}

		// FIXED: Uses getRealisticUser instead of r.Intn(1000) to force the engine
		// to allocate new entities at the same rate as the Ingest test.
		user := getRealisticUser(r)
		sb.WriteString(fmt.Sprintf("EVENT in:%s entity:%s type:heavy ", ns, user))

		for i := 0; i < 15; i++ {
			val := fmt.Sprintf("val_%d_%s", r.Intn(100), "abcdefgh")
			sb.WriteString(fmt.Sprintf("tag_%d:%s ", i, val))
		}
		return sb.String()
	}

	complexStats := runBenchPhase(cfg, complexGen, ExpectOK)

	fmt.Print("  Measuring Index Lag (Settling Time)... ")
	complexSettle, _ := measureBenchLag(cfg, ns)
	fmt.Printf("%v\n", complexSettle)

	results = append(results, summarizeBench("v1_Complex_Payload", complexStats, cfg.LoadDuration, complexSettle))

	return generateHTMLReport(cfg, hw, results)
}

// --- Standalone Benchmarking Engine ---

func runBenchPhase(cfg Config, cmdGenerator func(*rand.Rand, int) string, validator Validator) []*BenchWorkerStats {
	stats := make([]*BenchWorkerStats, cfg.LoadWorkers)
	for i := 0; i < cfg.LoadWorkers; i++ {
		stats[i] = &BenchWorkerStats{
			ID:        i,
			Latencies: make([]time.Duration, 0, BenchMaxLatencySamples),
		}
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	for i := 0; i < cfg.LoadWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			myStats := stats[id]
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			c, err := client.New(cfg.Address)
			if err != nil {
				myStats.ErrorCount++
				return
			}
			defer c.Close()

			for {
				select {
				case <-stopCh:
					return
				default:
					cmd := cmdGenerator(r, id)
					start := time.Now()

					if err := c.SendCommand(cmd); err != nil {
						myStats.ErrorCount++
						continue
					}

					resp, err := c.ReadResponse()
					dur := time.Since(start)

					if err != nil || (validator != nil && validator(resp) != nil) {
						myStats.ErrorCount++
						continue
					}

					myStats.SuccessCount++
					if len(myStats.Latencies) < BenchMaxLatencySamples {
						myStats.Latencies = append(myStats.Latencies, dur)
					} else {
						idx := r.Intn(int(myStats.SuccessCount))
						if idx < BenchMaxLatencySamples {
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

	return stats
}

func measureBenchLag(cfg Config, ns string) (time.Duration, error) {
	c, err := client.New(cfg.Address)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	uniqueProbeType := fmt.Sprintf("probe_type_%d", time.Now().UnixNano())
	start := time.Now()

	cmd := fmt.Sprintf("EVENT in:%s entity:probe_ent loc:probe type:%s", ns, uniqueProbeType)
	if err := c.SendCommand(cmd); err != nil {
		return 0, err
	}
	if _, err := c.ReadResponse(); err != nil {
		return 0, err
	}

	attempts := 12000
	for i := 0; i < attempts; i++ {
		c.SendCommand(fmt.Sprintf("QUERY in:%s where:(type:%s)", ns, uniqueProbeType))
		res, err := c.ReadResponse()
		if err == nil {
			if objs, _ := ExtractObjects(res); len(objs) > 0 {
				return time.Since(start), nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("timeout waiting for indexer")
}

func summarizeBench(name string, workers []*BenchWorkerStats, durationSec int, settle time.Duration) *BenchPhaseResult {
	res := &BenchPhaseResult{
		Name:     name,
		Duration: durationSec,
		LagMs:    float64(settle.Microseconds()) / 1000.0,
		Workers:  []BenchWorkerRow{},
	}

	var allLatencies []time.Duration
	for _, w := range workers {
		res.Aggregate.TotalOK += w.SuccessCount
		res.Aggregate.TotalErr += w.ErrorCount
		allLatencies = append(allLatencies, w.Latencies...)

		row := BenchWorkerRow{
			ID:           w.ID,
			SuccessCount: w.SuccessCount,
			ErrorCount:   w.ErrorCount,
		}

		if len(w.Latencies) > 0 {
			lats := make([]time.Duration, len(w.Latencies))
			copy(lats, w.Latencies)
			sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
			count := float64(len(lats))
			row.P50Ms = float64(lats[int(count*0.50)].Microseconds()) / 1000.0
			row.P90Ms = float64(lats[int(count*0.90)].Microseconds()) / 1000.0
			row.P95Ms = float64(lats[int(count*0.95)].Microseconds()) / 1000.0
			row.P99Ms = float64(lats[int(count*0.99)].Microseconds()) / 1000.0
			row.MaxMs = float64(lats[len(lats)-1].Microseconds()) / 1000.0
		}
		res.Workers = append(res.Workers, row)
	}

	res.Aggregate.TotalRPS = float64(res.Aggregate.TotalOK) / float64(durationSec)

	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })

	getPercMs := func(p float64) float64 {
		if len(allLatencies) == 0 {
			return 0
		}
		return float64(allLatencies[int(float64(len(allLatencies))*p)].Microseconds()) / 1000.0
	}

	if len(allLatencies) > 0 {
		res.Aggregate.P50Ms = getPercMs(0.50)
		res.Aggregate.P90Ms = getPercMs(0.90)
		res.Aggregate.P95Ms = getPercMs(0.95)
		res.Aggregate.P99Ms = getPercMs(0.99)
		res.Aggregate.MaxMs = float64(allLatencies[len(allLatencies)-1].Microseconds()) / 1000.0
	}

	return res
}

func getHardwareInfo() HardwareInfo {
	info := HardwareInfo{
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		LogicalCPUs: runtime.NumCPU(),
		CPUName:     "Unknown CPU",
	}

	if runtime.GOOS == "darwin" {
		out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
		if err == nil {
			info.CPUName = strings.TrimSpace(string(out))
		}
		out, err = exec.Command("sysctl", "-n", "hw.physicalcpu").Output()
		if err == nil {
			fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &info.PhysicalCPUs)
		}
		out, err = exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err == nil {
			var bytes int64
			fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &bytes)
			info.RAMTotalGB = float64(bytes) / (1024 * 1024 * 1024)
		}
	} else if runtime.GOOS == "linux" {
		out, err := exec.Command("sh", "-c", "cat /proc/cpuinfo | grep 'model name' | head -1 | awk -F': ' '{print $2}'").Output()
		if err == nil && len(out) > 0 {
			info.CPUName = strings.TrimSpace(string(out))
		}
		out, err = exec.Command("sh", "-c", "cat /proc/cpuinfo | grep 'core id' | sort -u | wc -l").Output()
		if err == nil && len(out) > 0 {
			fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &info.PhysicalCPUs)
		}
		out, err = exec.Command("sh", "-c", "awk '/MemTotal/ {print $2}' /proc/meminfo").Output()
		if err == nil {
			var kb int64
			fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &kb)
			info.RAMTotalGB = float64(kb) / (1024 * 1024)
		}
	}

	if info.PhysicalCPUs == 0 {
		info.PhysicalCPUs = info.LogicalCPUs
	}

	return info
}

func generateHTMLReport(cfg Config, hw HardwareInfo, results []*BenchPhaseResult) error {
	timestamp := time.Now().Format("20060102_150405")
	safeCPU := strings.ReplaceAll(strings.ToLower(hw.CPUName), " ", "_")
	safeCPU = strings.ReplaceAll(safeCPU, "(r)", "")
	safeCPU = strings.ReplaceAll(safeCPU, "(tm)", "")
	safeCPU = strings.ReplaceAll(safeCPU, "@", "")

	cleanCPU := ""
	for _, char := range safeCPU {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '_' {
			cleanCPU += string(char)
		}
	}
	for strings.Contains(cleanCPU, "__") {
		cleanCPU = strings.ReplaceAll(cleanCPU, "__", "_")
	}

	filename := fmt.Sprintf("benchmark_report_%s_%s.html", timestamp, cleanCPU)

	resultsJSON, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("failed to serialize chart data: %v", err)
	}

	data := BenchReportData{
		Timestamp:   time.Now().Format(time.RFC1123),
		Workers:     cfg.LoadWorkers,
		DurationSec: cfg.LoadDuration,
		Hardware:    hw,
		Results:     results,
		ResultsJSON: template.JS(string(resultsJSON)),
	}

	tmpl, err := template.New("report").Parse(benchHTMLTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse template: %v", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute template: %v", err)
	}

	if err := os.WriteFile(filename, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write report: %v", err)
	}

	fmt.Printf("\n✅ Benchmark Complete!\n")
	fmt.Printf("📊 Report saved to: %s\n", filename)
	return nil
}
