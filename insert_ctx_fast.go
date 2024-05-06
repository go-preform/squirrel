//go:build go1.8
// +build go1.8

package squirrel

import (
	"context"
	"database/sql"
)

func (d InsertBuilderFast) ExecContext(ctx context.Context) (sql.Result, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	ctxRunner, ok := d.runWith.(ExecerContext)
	if !ok {
		return nil, NoContextSupport
	}
	return ExecContextWith(ctx, ctxRunner, d)
}

func (d InsertBuilderFast) QueryContext(ctx context.Context) (*sql.Rows, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	ctxRunner, ok := d.runWith.(QueryerContext)
	if !ok {
		return nil, NoContextSupport
	}
	return QueryContextWith(ctx, ctxRunner, d)
}

func (d InsertBuilderFast) QueryRowContext(ctx context.Context) RowScanner {
	if d.runWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.runWith.(QueryRowerContext)
	if !ok {
		if _, ok := d.runWith.(QueryerContext); !ok {
			return &Row{err: RunnerNotQueryRunner}
		}
		return &Row{err: NoContextSupport}
	}
	return QueryRowContextWith(ctx, queryRower, d)
}

// ScanContext is a shortcut for QueryRowContext().Scan.
func (b InsertBuilderFast) ScanContext(ctx context.Context, dest ...interface{}) error {
	return b.QueryRowContext(ctx).Scan(dest...)
}
