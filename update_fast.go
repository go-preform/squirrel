package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

type UpdateBuilderFast struct {
	placeholderFormat PlaceholderFormat
	runWith           BaseRunner
	prefixes          []Sqlizer
	table             string
	setClauses        []setClause
	from              Sqlizer
	whereParts        []Sqlizer
	orderBys          []string
	limit             string
	offset            string
	suffixes          []Sqlizer
}

func (d UpdateBuilderFast) Exec() (sql.Result, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.runWith, d)
}

func (d UpdateBuilderFast) Query() (*sql.Rows, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.runWith, d)
}

func (d UpdateBuilderFast) QueryRow() RowScanner {
	if d.runWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.runWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d UpdateBuilderFast) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.table) == 0 {
		err = fmt.Errorf("update statements must specify a table")
		return
	}
	if len(d.setClauses) == 0 {
		err = fmt.Errorf("update statements must have at least one Set clause")
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

	sql.WriteString("UPDATE ")
	sql.WriteString(d.table)

	sql.WriteString(" SET ")
	setSqls := make([]string, len(d.setClauses))
	for i, setClause := range d.setClauses {
		var valSql string
		if vs, ok := setClause.value.(Sqlizer); ok {
			vsql, vargs, err := vs.ToSql()
			if err != nil {
				return "", nil, err
			}
			if _, ok := vs.(SelectBuilder); ok {
				valSql = fmt.Sprintf("(%s)", vsql)
			} else {
				valSql = vsql
			}
			args = append(args, vargs...)
		} else {
			valSql = "?"
			args = append(args, setClause.value)
		}
		setSqls[i] = fmt.Sprintf("%s = %s", setClause.column, valSql)
	}
	sql.WriteString(strings.Join(setSqls, ", "))

	if d.from != nil {
		sql.WriteString(" FROM ")
		args, err = appendToSql([]Sqlizer{d.from}, sql, "", args)
		if err != nil {
			return
		}
	}

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
func (b UpdateBuilderFast) PlaceholderFormat(f PlaceholderFormat) UpdateBuilderFast {
	b.placeholderFormat = f
	return b
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b UpdateBuilderFast) RunWith(runner BaseRunner) UpdateBuilderFast {
	b.runWith = runner
	return b
}

func (b UpdateBuilderFast) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b UpdateBuilderFast) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b UpdateBuilderFast) Prefix(sql string, args ...interface{}) UpdateBuilderFast {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b UpdateBuilderFast) PrefixExpr(expr Sqlizer) UpdateBuilderFast {
	b.prefixes = append(b.prefixes, expr)
	return b
}

// Table sets the table to be updated.
func (b UpdateBuilderFast) Table(table string) UpdateBuilderFast {
	b.table = table
	return b
}

// Set adds SET clauses to the query.
func (b UpdateBuilderFast) Set(column string, value interface{}) UpdateBuilderFast {
	b.setClauses = append(b.setClauses, setClause{column: column, value: value})
	return b
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b UpdateBuilderFast) SetMap(clauses map[string]interface{}) UpdateBuilderFast {
	keys := make([]string, len(clauses))
	i := 0
	for key := range clauses {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		val, _ := clauses[key]
		b = b.Set(key, val)
	}
	return b
}

// From adds FROM clause to the query
// FROM is valid construct in postgresql only.
func (b UpdateBuilderFast) From(from string) UpdateBuilderFast {
	b.from = newPart(from)
	return b
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b UpdateBuilderFast) FromSelect(from SelectBuilder, alias string) UpdateBuilderFast {
	// Prevent misnumbered parameters in nested selects (#183).
	from = from.PlaceholderFormat(Question)
	b.from = Alias(from, alias)
	return b
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b UpdateBuilderFast) Where(pred interface{}, args ...interface{}) UpdateBuilderFast {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b UpdateBuilderFast) OrderBy(orderBys ...string) UpdateBuilderFast {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b UpdateBuilderFast) Limit(limit uint64) UpdateBuilderFast {
	b.limit = fmt.Sprintf("%d", limit)
	return b
}

// Offset sets a OFFSET clause on the query.
func (b UpdateBuilderFast) Offset(offset uint64) UpdateBuilderFast {
	b.offset = fmt.Sprintf("%d", offset)
	return b
}

// Suffix adds an expression to the end of the query
func (b UpdateBuilderFast) Suffix(sql string, args ...interface{}) UpdateBuilderFast {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b UpdateBuilderFast) SuffixExpr(expr Sqlizer) UpdateBuilderFast {
	b.suffixes = append(b.suffixes, expr)
	return b
}
