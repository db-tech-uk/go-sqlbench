package benchmark

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
)

func BmSimpleExec(db *sql.DB, n int) error {
	ctx := context.Background()

	for i := 0; i < n; i++ {
		if _, err := db.ExecContext(ctx, "DO 1"); err != nil {
			return err
		}
	}
	return nil
}

func BmPreparedExec(db *sql.DB, n int) error {
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "DO 1")
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		if _, err := stmt.ExecContext(ctx); err != nil {
			return err
		}
	}

	return stmt.Close()
}

func BmSimpleQueryRow(db *sql.DB, n int) error {
	ctx := context.Background()

	var num int

	for i := 0; i < n; i++ {
		if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&num); err != nil {
			return err
		}
	}
	return nil
}

func BmPreparedQueryRow(db *sql.DB, n int) error {
	ctx := context.Background()

	var num int

	stmt, err := db.PrepareContext(ctx, "SELECT 1")
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		if err := stmt.QueryRowContext(ctx).Scan(&num); err != nil {
			return err
		}
	}

	return stmt.Close()
}

func BmPreparedQueryRowParam(db *sql.DB, n int) error {
	ctx := context.Background()

	var num int

	stmt, err := db.PrepareContext(ctx, "SELECT ?")
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		if err := stmt.QueryRowContext(ctx, i).Scan(&num); err != nil {
			return err
		}
	}

	return stmt.Close()
}

func BmEchoMixed5(db *sql.DB, n int) error {
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "SELECT ?, ?, ?, ?, ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Some random data with different types
	type entry struct {
		id    int64
		name  string
		ratio float64
		other interface{}
		hire  bool
	}

	in := entry{
		id:    42,
		name:  "Gopher",
		ratio: 1.618,
		other: nil,
		hire:  true,
	}

	var out entry

	for i := 0; i < n; i++ {
		if err := stmt.QueryRowContext(
			ctx,
			in.id,
			in.name,
			in.ratio,
			in.other,
			in.hire,
		).Scan(
			&out.id,
			&out.name,
			&out.ratio,
			&out.other,
			&out.hire,
		); err != nil {
			return err
		}
	}
	return nil
}

func BmSelectLargeString(db *sql.DB, n int) error {
	ctx := context.Background()

	var str string
	for i := 0; i < n; i++ {
		if err := db.QueryRowContext(ctx, "SELECT REPEAT('A', 10000)").Scan(&str); err != nil {
			return err
		}
	}
	return nil
}

func BmSelectPreparedLargeString(db *sql.DB, n int) error {
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "SELECT REPEAT('A', 10000)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	var str string
	for i := 0; i < n; i++ {
		if err := stmt.QueryRowContext(ctx).Scan(&str); err != nil {
			return err
		}
	}
	return nil
}

func BmSelectLargeBytes(db *sql.DB, n int) error {
	ctx := context.Background()

	var raw []byte
	for i := 0; i < n; i++ {
		if err := db.QueryRowContext(ctx, "SELECT REPEAT('A', 10000)").Scan(&raw); err != nil {
			return err
		}
	}
	return nil
}

func BmSelectPreparedLargeBytes(db *sql.DB, n int) error {
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "SELECT REPEAT('A', 10000)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	var raw []byte
	for i := 0; i < n; i++ {
		if err := stmt.QueryRowContext(ctx).Scan(&raw); err != nil {
			return err
		}
	}
	return nil
}

func BmSelectLargeRaw(db *sql.DB, n int) error {
	ctx := context.Background()

	var raw sql.RawBytes
	for i := 0; i < n; i++ {
		rows, err := db.QueryContext(ctx, "SELECT REPEAT('A', 10000)")
		if err != nil {
			return err
		}

		if !rows.Next() {
			return sql.ErrNoRows
		}

		if err = rows.Scan(&raw); err != nil {
			return err
		}

		if err = rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func BmSelectPreparedLargeRaw(db *sql.DB, n int) error {
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "SELECT REPEAT('A', 10000)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	var raw sql.RawBytes
	for i := 0; i < n; i++ {
		rows, err := stmt.QueryContext(ctx)
		if err != nil {
			return err
		}

		if !rows.Next() {
			return sql.ErrNoRows
		}

		if err = rows.Scan(&raw); err != nil {
			return err
		}

		if err = rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

// runConcurrent calls fn n times in total, spread across co goroutines.
// The first error cancels the remaining work and is returned. If the parent
// context is cancelled before all the work has run, its error is returned.
func runConcurrent(parent context.Context, n, co int, fn func(ctx context.Context, worker int) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)

	remain := int64(n)

	wg.Add(co)

	for w := 0; w < co; w++ {
		go func(w int) {
			defer wg.Done()

			for ctx.Err() == nil && atomic.AddInt64(&remain, -1) >= 0 {
				if err := fn(ctx, w); err != nil {
					once.Do(func() {
						firstErr = err
						cancel()
					})

					return
				}
			}
		}(w)
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	// remain only stays above zero when the loop stopped before claiming every task.
	if atomic.LoadInt64(&remain) > 0 {
		return parent.Err()
	}

	return nil
}

func runPreparedExecConcurrent(db *sql.DB, n int, co int) error {
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "DO 1")
	if err != nil {
		return err
	}

	runErr := runConcurrent(ctx, n, co, func(ctx context.Context, _ int) error {
		_, err := stmt.ExecContext(ctx)

		return err
	})

	return errors.Join(runErr, stmt.Close())
}

func BmPreparedExecConcurrent1(db *sql.DB, n int) error {
	return runPreparedExecConcurrent(db, n, 1)
}

func BmPreparedExecConcurrent2(db *sql.DB, n int) error {
	return runPreparedExecConcurrent(db, n, 2)
}

func BmPreparedExecConcurrent4(db *sql.DB, n int) error {
	return runPreparedExecConcurrent(db, n, 4)
}

func BmPreparedExecConcurrent8(db *sql.DB, n int) error {
	return runPreparedExecConcurrent(db, n, 8)
}

func BmPreparedExecConcurrent16(db *sql.DB, n int) error {
	return runPreparedExecConcurrent(db, n, 16)
}

func runPreparedQueryConcurrent(db *sql.DB, n int, co int) error {
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "SELECT ?, \"foobar\"")
	if err != nil {
		return err
	}

	runErr := runConcurrent(ctx, n, co, func(ctx context.Context, worker int) error {
		var id int
		var str string

		return stmt.QueryRowContext(ctx, worker).Scan(&id, &str)
	})

	return errors.Join(runErr, stmt.Close())
}

func BmPreparedQueryConcurrent1(db *sql.DB, n int) error {
	return runPreparedQueryConcurrent(db, n, 1)
}

func BmPreparedQueryConcurrent2(db *sql.DB, n int) error {
	return runPreparedQueryConcurrent(db, n, 2)
}

func BmPreparedQueryConcurrent4(db *sql.DB, n int) error {
	return runPreparedQueryConcurrent(db, n, 4)
}

func BmPreparedQueryConcurrent8(db *sql.DB, n int) error {
	return runPreparedQueryConcurrent(db, n, 8)
}

func BmPreparedQueryConcurrent16(db *sql.DB, n int) error {
	return runPreparedQueryConcurrent(db, n, 16)
}
