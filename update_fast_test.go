package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateBuilderFastToSql(t *testing.T) {
	b := UpdateFast("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Set("c1", Case("status").When("1", "2").When("2", "1")).
		Set("c2", Case().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
		Set("c3", Select("a").From("b")).
		Where("d = ?", 3).
		OrderBy("e").
		Limit(4).
		Offset(5).
		Suffix("RETURNING ?", 6)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"UPDATE a SET b = ? + 1, c = ?, " +
			"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
			"c2 = CASE WHEN a = 2 THEN ? WHEN a = 3 THEN ? END, " +
			"c3 = (SELECT a FROM b) " +
			"WHERE d = ? " +
			"ORDER BY e LIMIT 4 OFFSET 5 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2, "foo", "bar", 3, 6}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderFastToSqlErr(t *testing.T) {
	_, _, err := UpdateFast("").Set("x", 1).ToSql()
	assert.Error(t, err)

	_, _, err = UpdateFast("x").ToSql()
	assert.Error(t, err)
}

func TestUpdateBuilderFastMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestUpdateBuilderFastMustSql should have panicked!")
		}
	}()
	UpdateFast("").MustSql()
}

func TestUpdateBuilderFastPlaceholders(t *testing.T) {
	b := UpdateFast("test").SetMap(Eq{"x": 1, "y": 2})

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "UPDATE test SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "UPDATE test SET x = $1, y = $2", sql)
}

func TestUpdateBuilderFastRunners(t *testing.T) {
	db := &DBStub{}
	b := UpdateFast("test").Set("x", 1).RunWith(db)

	expectedSql := "UPDATE test SET x = ?"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestUpdateBuilderFastNoRunner(t *testing.T) {
	b := UpdateFast("test").Set("x", 1)

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestUpdateBuilderFastFrom(t *testing.T) {
	sql, _, err := UpdateFast("employees").Set("sales_count", 100).From("accounts").Where("accounts.name = ?", "ACME").ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE employees SET sales_count = ? FROM accounts WHERE accounts.name = ?", sql)
}

func TestUpdateBuilderFastFromSelect(t *testing.T) {
	sql, _, err := UpdateFast("employees").
		Set("sales_count", 100).
		FromSelect(Select("id").
			From("accounts").
			Where("accounts.name = ?", "ACME"), "subquery").
		Where("employees.account_id = subquery.id").ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"UPDATE employees " +
			"SET sales_count = ? " +
			"FROM (SELECT id FROM accounts WHERE accounts.name = ?) AS subquery " +
			"WHERE employees.account_id = subquery.id"
	assert.Equal(t, expectedSql, sql)
}

func BenchmarkUpdate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Update("").
			Prefix("WITH prefix AS ?", 0).
			Table("a").
			Set("b", Expr("? + 1", 1)).
			SetMap(Eq{"c": 2}).
			Set("c1", Case("status").When("1", "2").When("2", "1")).
			Set("c2", Case().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
			Set("c3", Select("a").From("b")).
			Where("d = ?", 3).
			OrderBy("e").
			Limit(4).
			Offset(5).
			Suffix("RETURNING ?", 6).ToSql()
	}
}

func BenchmarkUpdateFast(b *testing.B) {
	for i := 0; i < b.N; i++ {
		UpdateFast("").
			Prefix("WITH prefix AS ?", 0).
			Table("a").
			Set("b", Expr("? + 1", 1)).
			SetMap(Eq{"c": 2}).
			Set("c1", CaseFast("status").When("1", "2").When("2", "1")).
			Set("c2", CaseFast().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
			Set("c3", SelectFast("a").From("b")).
			Where("d = ?", 3).
			OrderBy("e").
			Limit(4).
			Offset(5).
			Suffix("RETURNING ?", 6).ToSql()
	}
}
