package squirrel

import (
	"errors"
)

// sqlizerBuffer is a helper that allows to write many Sqlizers one by one
// without constant checks for errors that may come from Sqlizer

// whenPart is a helper structure to describe SQLs "WHEN ... THEN ..." expression

// CaseBuilderFast holds all the data required to build a CASE SQL construct
type CaseBuilderFast struct {
	whatSqlizer Sqlizer
	whenParts   []whenPart
	elseSqlizer Sqlizer
}

// ToSql implements Sqlizer
func (d CaseBuilderFast) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.whenParts) == 0 {
		err = errors.New("case expression must contain at lease one WHEN clause")

		return
	}

	sql := sqlizerBuffer{}

	sql.WriteString("CASE ")
	if d.whatSqlizer != nil {
		sql.WriteSql(d.whatSqlizer)
	}

	for _, p := range d.whenParts {
		sql.WriteString("WHEN ")
		sql.WriteSql(p.when)
		sql.WriteString("THEN ")
		sql.WriteSql(p.then)
	}

	if d.elseSqlizer != nil {
		sql.WriteString("ELSE ")
		sql.WriteSql(d.elseSqlizer)
	}

	sql.WriteString("END")

	return sql.ToSql()
}

// CaseBuilder builds SQL CASE construct which could be used as parts of queries.

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b CaseBuilderFast) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// what sets optional value for CASE construct "CASE [value] ..."
func (b CaseBuilderFast) what(expr interface{}) CaseBuilderFast {
	b.whatSqlizer = newPart(expr)
	return b
}

// When adds "WHEN ... THEN ..." part to CASE construct
func (b CaseBuilderFast) When(when interface{}, then interface{}) CaseBuilderFast {
	b.whenParts = append(b.whenParts, newWhenPart(when, then))
	return b
}

// What sets optional "ELSE ..." part for CASE construct
func (b CaseBuilderFast) Else(expr interface{}) CaseBuilderFast {
	b.elseSqlizer = newPart(expr)
	return b
}
