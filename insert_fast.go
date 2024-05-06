package squirrel

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

type InsertBuilderFast struct {
	placeholderFormat   PlaceholderFormat
	runWith             BaseRunner
	prefixes            []Sqlizer
	statementKeywordStr string
	options             []string
	into                string
	columns             []string
	values              [][]interface{}
	suffixes            []Sqlizer
	selectBuilder       Sqlizer
}

func (d InsertBuilderFast) Exec() (sql.Result, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.runWith, d)
}

func (d InsertBuilderFast) Query() (*sql.Rows, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.runWith, d)
}

func (d InsertBuilderFast) QueryRow() RowScanner {
	if d.runWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.runWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d InsertBuilderFast) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.into) == 0 {
		err = errors.New("insert statements must specify a table")
		return
	}
	if len(d.values) == 0 && d.selectBuilder == nil {
		err = errors.New("insert statements must have at least one set of values or select clause")
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

	if d.statementKeywordStr == "" {
		sql.WriteString("INSERT ")
	} else {
		sql.WriteString(d.statementKeywordStr)
		sql.WriteString(" ")
	}

	if len(d.options) > 0 {
		sql.WriteString(strings.Join(d.options, " "))
		sql.WriteString(" ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(d.into)
	sql.WriteString(" ")

	if len(d.columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(d.columns, ","))
		sql.WriteString(") ")
	}

	if d.selectBuilder != nil {
		args, err = d.appendSelectToSQL(sql, args)
	} else {
		args, err = d.appendValuesToSQL(sql, args)
	}
	if err != nil {
		return
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

func (d InsertBuilderFast) appendValuesToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if len(d.values) == 0 {
		return args, errors.New("values for insert statements are not set")
	}

	io.WriteString(w, "VALUES ")

	valuesStrings := make([]string, len(d.values))
	for r, row := range d.values {
		valueStrings := make([]string, len(row))
		for v, val := range row {
			if vs, ok := val.(Sqlizer); ok {
				vsql, vargs, err := vs.ToSql()
				if err != nil {
					return nil, err
				}
				valueStrings[v] = vsql
				args = append(args, vargs...)
			} else {
				valueStrings[v] = "?"
				args = append(args, val)
			}
		}
		valuesStrings[r] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ","))
	}

	io.WriteString(w, strings.Join(valuesStrings, ","))

	return args, nil
}

func (d InsertBuilderFast) appendSelectToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if d.selectBuilder == nil {
		return args, errors.New("select clause for insert statements are not set")
	}

	selectClause, sArgs, err := d.selectBuilder.ToSql()
	if err != nil {
		return args, err
	}

	io.WriteString(w, selectClause)
	args = append(args, sArgs...)

	return args, nil
}

// Builder

// InsertBuilderFast builds SQL INSERT statements.
//type InsertBuilderFast builder.Builder

//func init() {
//	builder.Register(InsertBuilderFast{}, InsertBuilderFast{})
//}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b InsertBuilderFast) PlaceholderFormat(f PlaceholderFormat) InsertBuilderFast {
	b.placeholderFormat = f
	return b
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b InsertBuilderFast) RunWith(runner BaseRunner) InsertBuilderFast {
	b.runWith = runner
	return b
}

// Scan is a shortcut for QueryRow().Scan.
func (b InsertBuilderFast) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b InsertBuilderFast) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b InsertBuilderFast) Prefix(sql string, args ...interface{}) InsertBuilderFast {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b InsertBuilderFast) PrefixExpr(expr Sqlizer) InsertBuilderFast {
	b.prefixes = append(b.prefixes, expr)
	return b
}

// Options adds keyword options before the INTO clause of the query.
func (b InsertBuilderFast) Options(options ...string) InsertBuilderFast {
	b.options = append(b.options, options...)
	return b
}

// Into sets the INTO clause of the query.
func (b InsertBuilderFast) Into(from string) InsertBuilderFast {
	b.into = from
	return b
}

// Columns adds insert columns to the query.
func (b InsertBuilderFast) Columns(columns ...string) InsertBuilderFast {
	b.columns = append(b.columns, columns...)
	return b
}

// Values adds a single row's values to the query.
func (b InsertBuilderFast) Values(values ...interface{}) InsertBuilderFast {
	b.values = append(b.values, values)
	return b
}

// Suffix adds an expression to the end of the query
func (b InsertBuilderFast) Suffix(sql string, args ...interface{}) InsertBuilderFast {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b InsertBuilderFast) SuffixExpr(expr Sqlizer) InsertBuilderFast {
	b.suffixes = append(b.suffixes, expr)
	return b
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b InsertBuilderFast) SetMap(clauses map[string]interface{}) InsertBuilderFast {
	// Keep the columns in a consistent order by sorting the column key string.
	cols := make([]string, 0, len(clauses))
	for col := range clauses {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	vals := make([]interface{}, 0, len(clauses))
	for _, col := range cols {
		vals = append(vals, clauses[col])
	}

	b.columns = append(b.columns, cols...)
	b.values = append(b.values, vals)

	return b
}

// Select set Select clause for insert query
// If Values and Select are used, then Select has higher priority
func (b InsertBuilderFast) Select(sb Sqlizer) InsertBuilderFast {
	b.selectBuilder = sb
	return b
}

func (b InsertBuilderFast) statementKeyword(keyword string) InsertBuilderFast {
	b.statementKeywordStr = keyword
	return b
}
