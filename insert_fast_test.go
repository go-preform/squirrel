package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertFastBuilderToSql(t *testing.T) {
	b := InsertFast("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL :=
		"WITH prefix AS ? " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES (?,?),(?,? + 1) " +
			"RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertFastBuilderToSqlErr(t *testing.T) {
	_, _, err := InsertFast("").Values(1).ToSql()
	assert.Error(t, err)

	_, _, err = InsertFast("x").ToSql()
	assert.Error(t, err)
}

func TestInsertFastBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestInsertFastBuilderMustSql should have panicked!")
		}
	}()
	InsertFast("").MustSql()
}

func TestInsertFastBuilderPlaceholders(t *testing.T) {
	b := InsertFast("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertFastBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := InsertFast("test").Values(1).RunWith(db)

	expectedSQL := "INSERT INTO test VALUES (?)"

	b.Exec()
	assert.Equal(t, expectedSQL, db.LastExecSql)
}

func TestInsertFastBuilderNoRunner(t *testing.T) {
	b := InsertFast("test").Values(1)

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestInsertFastBuilderSetMap(t *testing.T) {
	b := InsertFast("table").SetMap(Eq{"field1": 1, "field2": 2, "field3": 3})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table (field1,field2,field3) VALUES (?,?,?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertFastBuilderSelect(t *testing.T) {
	sb := SelectFast("field1").From("table1").Where(Eq{"field1": 1})
	ib := InsertFast("table2").Columns("field1").Select(sb)

	sql, args, err := ib.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertFastBuilderReplace(t *testing.T) {
	b := ReplaceFast("table").Values(1)

	expectedSQL := "REPLACE INTO table VALUES (?)"

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
}

func BenchmarkInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Insert("").
			Prefix("WITH prefix AS ?", 0).
			Into("a").
			Options("DELAYED", "IGNORE").
			Columns("b", "c").
			Values(1, 2).
			Values(3, Expr("? + 1", 4)).
			Suffix("RETURNING ?", 5).ToSql()
	}
}

func BenchmarkInsertFast(b *testing.B) {
	for i := 0; i < b.N; i++ {
		InsertFast("").
			Prefix("WITH prefix AS ?", 0).
			Into("a").
			Options("DELAYED", "IGNORE").
			Columns("b", "c").
			Values(1, 2).
			Values(3, Expr("? + 1", 4)).
			Suffix("RETURNING ?", 5).ToSql()
	}
}
