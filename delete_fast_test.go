package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilderFastToSql(t *testing.T) {
	b := DeleteFast("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c").
		Limit(2).
		Offset(3).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"DELETE FROM a WHERE b = ? ORDER BY c LIMIT 2 OFFSET 3 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderFastToSqlErr(t *testing.T) {
	_, _, err := DeleteFast("").ToSql()
	assert.Error(t, err)
}

func TestDeleteBuilderFastMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestDeleteBuilderFastMustSql should have panicked!")
		}
	}()
	DeleteFast("").MustSql()
}

func TestDeleteBuilderFastPlaceholders(t *testing.T) {
	b := DeleteFast("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = $1 AND y = $2", sql)
}

func TestDeleteBuilderFastRunners(t *testing.T) {
	db := &DBStub{}
	b := DeleteFast("test").Where("x = ?", 1).RunWith(db)

	expectedSql := "DELETE FROM test WHERE x = ?"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestDeleteBuilderFastNoRunner(t *testing.T) {
	b := DeleteFast("test")

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestDeleteWithQueryFast(t *testing.T) {
	db := &DBStub{}
	b := DeleteFast("test").Where("id=55").Suffix("RETURNING path").RunWith(db)

	expectedSql := "DELETE FROM test WHERE id=55 RETURNING path"
	b.Query()

	assert.Equal(t, expectedSql, db.LastQuerySql)
}

func BenchmarkDelete(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Delete("").
			Prefix("WITH prefix AS ?", 0).
			From("a").
			Where("b = ?", 1).
			OrderBy("c").
			Limit(2).
			Offset(3).
			Suffix("RETURNING ?", 4).ToSql()
	}
}

func BenchmarkDeleteFast(b *testing.B) {
	for i := 0; i < b.N; i++ {
		DeleteFast("").
			Prefix("WITH prefix AS ?", 0).
			From("a").
			Where("b = ?", 1).
			OrderBy("c").
			Limit(2).
			Offset(3).
			Suffix("RETURNING ?", 4).ToSql()
	}
}
