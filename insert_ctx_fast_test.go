//go:build go1.8
// +build go1.8

package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertBuilderFastContextRunners(t *testing.T) {
	db := &DBStub{}
	b := InsertFast("test").Values(1).RunWith(db)

	expectedSql := "INSERT INTO test VALUES (?)"

	b.ExecContext(ctx)
	assert.Equal(t, expectedSql, db.LastExecSql)

	b.QueryContext(ctx)
	assert.Equal(t, expectedSql, db.LastQuerySql)

	b.QueryRowContext(ctx)
	assert.Equal(t, expectedSql, db.LastQueryRowSql)

	err := b.ScanContext(ctx)
	assert.NoError(t, err)
}

func TestInsertBuilderFastContextNoRunner(t *testing.T) {
	b := InsertFast("test").Values(1)

	_, err := b.ExecContext(ctx)
	assert.Equal(t, RunnerNotSet, err)

	_, err = b.QueryContext(ctx)
	assert.Equal(t, RunnerNotSet, err)

	err = b.ScanContext(ctx)
	assert.Equal(t, RunnerNotSet, err)
}
