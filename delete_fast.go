package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
)

type DeleteBuilderFast struct {
	placeholderFormat PlaceholderFormat
	runWith           BaseRunner
	prefixes          []Sqlizer
	from              string
	whereParts        []Sqlizer
	orderBys          []string
	limit             string
	offset            string
	suffixes          []Sqlizer
}

func (d DeleteBuilderFast) Exec() (sql.Result, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.runWith, d)
}

func (d DeleteBuilderFast) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.from) == 0 {
		err = fmt.Errorf("delete statements must specify a From table")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.prefixes) > 0 {
		args, err = appendToSql(d.prefixes, sql, " ", args)
		if err != nil {
			return
		}

		sql.WriteString(" ")
	}

	sql.WriteString("DELETE FROM ")
	sql.WriteString(d.from)

	if len(d.whereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSql(d.whereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(d.orderBys, ", "))
	}

	if len(d.limit) > 0 {
		sql.WriteString(" LIMIT ")
		sql.WriteString(d.limit)
	}

	if len(d.offset) > 0 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(d.offset)
	}

	if len(d.suffixes) > 0 {
		sql.WriteString(" ")
		args, err = appendToSql(d.suffixes, sql, " ", args)
		if err != nil {
			return
		}
	}

	sqlStr, err = d.placeholderFormat.ReplacePlaceholders(sql.String())
	return
}

// Builder

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b DeleteBuilderFast) PlaceholderFormat(f PlaceholderFormat) DeleteBuilderFast {
	b.placeholderFormat = f
	return b
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b DeleteBuilderFast) RunWith(runner BaseRunner) DeleteBuilderFast {
	b.runWith = runner
	return b
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b DeleteBuilderFast) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b DeleteBuilderFast) Prefix(sql string, args ...interface{}) DeleteBuilderFast {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b DeleteBuilderFast) PrefixExpr(expr Sqlizer) DeleteBuilderFast {
	b.prefixes = append(b.prefixes, expr)
	return b
}

// From sets the table to be deleted from.
func (b DeleteBuilderFast) From(from string) DeleteBuilderFast {
	b.from = from
	return b
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b DeleteBuilderFast) Where(pred interface{}, args ...interface{}) DeleteBuilderFast {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b DeleteBuilderFast) OrderBy(orderBys ...string) DeleteBuilderFast {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b DeleteBuilderFast) Limit(limit uint64) DeleteBuilderFast {
	b.limit = fmt.Sprintf("%d", limit)
	return b
}

// Offset sets a OFFSET clause on the query.
func (b DeleteBuilderFast) Offset(offset uint64) DeleteBuilderFast {
	b.offset = fmt.Sprintf("%d", offset)
	return b
}

// Suffix adds an expression to the end of the query
func (b DeleteBuilderFast) Suffix(sql string, args ...interface{}) DeleteBuilderFast {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b DeleteBuilderFast) SuffixExpr(expr Sqlizer) DeleteBuilderFast {
	b.suffixes = append(b.suffixes, expr)
	return b
}

func (b DeleteBuilderFast) Query() (*sql.Rows, error) {
	if b.runWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(b.runWith, b)
}
