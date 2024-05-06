package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCaseFastWithVal(t *testing.T) {
	caseStmt := CaseFast("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	qb := SelectFast().
		Column(caseStmt).
		From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE number " +
		"WHEN 1 THEN one " +
		"WHEN 2 THEN two " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"big number"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseFastWithComplexVal(t *testing.T) {
	caseStmt := CaseFast("? > ?", 10, 5).
		When("true", "'T'")

	qb := SelectFast().
		Column(Alias(caseStmt, "complexCase")).
		From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT (CASE ? > ? " +
		"WHEN true THEN 'T' " +
		"END) AS complexCase " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{10, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseFastWithNoVal(t *testing.T) {
	caseStmt := CaseFast().
		When(Eq{"x": 0}, "x is zero").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := SelectFast().Column(caseStmt).From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE " +
		"WHEN x = ? THEN x is zero " +
		"WHEN x > ? THEN CONCAT('x is greater than ', ?) " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseFastWithExpr(t *testing.T) {
	caseStmt := CaseFast(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")

	qb := SelectFast().Column(caseStmt).From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE x = ? " +
		"WHEN true THEN ? " +
		"ELSE 42 " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{true, "it's true!"}
	assert.Equal(t, expectedArgs, args)
}

func TestMultipleCaseFast(t *testing.T) {
	caseStmtNoval := CaseFast(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")
	caseStmtExpr := CaseFast().
		When(Eq{"x": 0}, "'x is zero'").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := SelectFast().
		Column(Alias(caseStmtNoval, "case_noval")).
		Column(Alias(caseStmtExpr, "case_expr")).
		From("table")

	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT " +
		"(CASE x = ? WHEN true THEN ? ELSE 42 END) AS case_noval, " +
		"(CASE WHEN x = ? THEN 'x is zero' WHEN x > ? THEN CONCAT('x is greater than ', ?) END) AS case_expr " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{
		true, "it's true!",
		0, 1, 2,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseFastWithNoWhenClause(t *testing.T) {
	caseStmt := CaseFast("something").
		Else("42")

	qb := SelectFast().Column(caseStmt).From("table")

	_, _, err := qb.ToSql()

	assert.Error(t, err)

	assert.Equal(t, "case expression must contain at lease one WHEN clause", err.Error())
}

func TestCaseFastBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestCaseFastBuilderMustSql should have panicked!")
		}
	}()
	CaseFast("").MustSql()
}
