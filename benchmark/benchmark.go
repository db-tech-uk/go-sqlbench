package benchmark

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"time"

	"github.com/db-tech-uk/go-utils/timeutils"
	"github.com/jmoiron/sqlx"
)

const benchTableName string = "benchmark"

type BenchmarkSuite struct {
	WarmUp     func(*sqlx.DB) error
	PrintStats bool
}

type BenchmarkResult struct {
	Duration time.Duration
	Queries  int
	Allocs   uint64
	Bytes    uint64
	Err      error
}

type Benchmark struct {
	Name string
	N    int
	Bm   func(*sqlx.DB, int) error
}

func (r BenchmarkResult) String() string {
	return fmt.Sprintf("\t\t\t\t"+"Duration: %v"+"\n"+
		"\t\t\t\t"+"Queries: %v"+"\n"+
		"\t\t\t\t"+"Allocs: %v"+"\n"+
		"\t\t\t\t"+"Bytes: %v"+"\n"+
		"\t\t\t\t"+"Err: %v",
		timeutils.FormatDuration(r.Duration, 2),
		r.Queries,
		r.Allocs,
		r.Bytes,
		r.Err,
	)
}

func (res *BenchmarkResult) QueriesPerSecond() float64 {
	return float64(res.Queries) / res.Duration.Seconds()
}

func (res *BenchmarkResult) AllocsPerQuery() int {
	return perQuery(res.Allocs, res.Queries)
}

func (res *BenchmarkResult) BytesPerQuery() int {
	return perQuery(res.Bytes, res.Queries)
}

// perQuery divides total by queries, guarding against division by zero and
// clamping the result so the uint64 -> int conversion cannot overflow.
func perQuery(total uint64, queries int) int {
	if queries <= 0 {
		return 0
	}

	v := total / uint64(queries)
	if v > math.MaxInt {
		return math.MaxInt
	}

	return int(v)
}

func (b *Benchmark) Run(db *sqlx.DB) BenchmarkResult {
	runtime.GC()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	startMallocs := memStats.Mallocs
	startTotalAlloc := memStats.TotalAlloc
	startTime := time.Now()

	err := b.Bm(db, b.N)

	endTime := time.Now()
	runtime.ReadMemStats(&memStats)

	return BenchmarkResult{
		Err:      err,
		Queries:  b.N,
		Duration: endTime.Sub(startTime),
		Allocs:   memStats.Mallocs - startMallocs,
		Bytes:    memStats.TotalAlloc - startTotalAlloc,
	}
}

func Warmup(db *sqlx.DB) error {
	db.SetMaxIdleConns(16)

	ctx := context.Background()

	for i := 0; i < 10000; i++ {
		rows, err := db.QueryContext(ctx, "SELECT 5 * 3")
		if err != nil {
			return err
		}

		if err = rows.Close(); err != nil {
			return err
		}
	}

	return nil
}
