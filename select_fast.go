package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
)

type SelectBuilderFast struct {
	placeholderFormat PlaceholderFormat
	runWith           BaseRunner
	prefixes          []Sqlizer
	options           []string
	columns           []Sqlizer
	from              Sqlizer
	joins             []Sqlizer
	whereParts        []Sqlizer
	groupBys          []string
	havingParts       []Sqlizer
	orderByParts      []Sqlizer
	limit             string
	offset            string
	suffixes          []Sqlizer
}

func (d *SelectBuilderFast) Exec() (sql.Result, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.runWith, d)
}

func (d *SelectBuilderFast) Query() (*sql.Rows, error) {
	if d.runWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.runWith, d)
}

func (d *SelectBuilderFast) QueryRow() RowScanner {
	if d.runWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.runWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d SelectBuilderFast) ToSql() (sqlStr string, args []interface{}, err error) {
	sqlStr, args, err = d.toSqlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.placeholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d SelectBuilderFast) toSqlRaw() (sqlStr string, args []interface{}, err error) {
	if len(d.columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
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

	sql.WriteString("SELECT ")

	if len(d.options) > 0 {
		sql.WriteString(strings.Join(d.options, " "))
		sql.WriteString(" ")
	}

	if len(d.columns) > 0 {
		args, err = appendToSql(d.columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if d.from != nil {
		sql.WriteString(" FROM ")
		args, err = appendToSql([]Sqlizer{d.from}, sql, "", args)
		if err != nil {
			return
		}
	}

	if len(d.joins) > 0 {
		sql.WriteString(" ")
		args, err = appendToSql(d.joins, sql, " ", args)
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

	if len(d.groupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(d.groupBys, ", "))
	}

	if len(d.havingParts) > 0 {
		sql.WriteString(" HAVING ")
		args, err = appendToSql(d.havingParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.orderByParts) > 0 {
		sql.WriteString(" ORDER BY ")
		args, err = appendToSql(d.orderByParts, sql, ", ", args)
		if err != nil {
			return
		}
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

	sqlStr = sql.String()
	return
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b SelectBuilderFast) PlaceholderFormat(f PlaceholderFormat) SelectBuilderFast {
	b.placeholderFormat = f
	return b
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
// For most cases runner will be a database connection.
//
// Internally we use this to mock out the database connection for testing.
func (b SelectBuilderFast) RunWith(runner BaseRunner) SelectBuilderFast {
	b.runWith = runner
	return b
}

// Scan is a shortcut for QueryRow().Scan.
func (b SelectBuilderFast) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b SelectBuilderFast) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b SelectBuilderFast) Prefix(sql string, args ...interface{}) SelectBuilderFast {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b SelectBuilderFast) PrefixExpr(expr Sqlizer) SelectBuilderFast {
	b.prefixes = append(b.prefixes, expr)
	return b
}

// Distinct adds a DISTINCT clause to the query.
func (b SelectBuilderFast) Distinct() SelectBuilderFast {
	return b.Options("DISTINCT")
}

// Options adds select option to the query
func (b SelectBuilderFast) Options(options ...string) SelectBuilderFast {
	b.options = append(b.options, options...)
	return b
}

// Columns adds result columns to the query.
func (b SelectBuilderFast) Columns(columns ...string) SelectBuilderFast {
	parts := make([]Sqlizer, len(columns))
	for i, str := range columns {
		parts[i] = newPart(str)
	}
	b.columns = append(b.columns, parts...)
	return b
}

// RemoveColumns remove all columns from query.
// Must add a new column with Column or Columns methods, otherwise
// return a error.
func (b SelectBuilderFast) RemoveColumns() SelectBuilderFast {
	b.columns = nil
	return b
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//
//	Column("IF(col IN ("+squirrel.Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b SelectBuilderFast) Column(column interface{}, args ...interface{}) SelectBuilderFast {
	b.columns = append(b.columns, newPart(column, args...))
	return b
}

// From sets the FROM clause of the query.
func (b SelectBuilderFast) From(from string) SelectBuilderFast {
	b.from = newPart(from)
	return b
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b SelectBuilderFast) FromSelect(from SelectBuilder, alias string) SelectBuilderFast {
	// Prevent misnumbered parameters in nested selects (#183).
	b.from = Alias(from.PlaceholderFormat(Question), alias)
	return b
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b SelectBuilderFast) FromSelectFast(from SelectBuilderFast, alias string) SelectBuilderFast {
	// Prevent misnumbered parameters in nested selects (#183).
	b.from = Alias(from.PlaceholderFormat(Question), alias)
	return b
}

// JoinClause adds a join clause to the query.
func (b SelectBuilderFast) JoinClause(pred interface{}, args ...interface{}) SelectBuilderFast {
	b.joins = append(b.joins, newPart(pred, args...))
	return b
}

// Join adds a JOIN clause to the query.
func (b SelectBuilderFast) Join(join string, rest ...interface{}) SelectBuilderFast {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b SelectBuilderFast) LeftJoin(join string, rest ...interface{}) SelectBuilderFast {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b SelectBuilderFast) RightJoin(join string, rest ...interface{}) SelectBuilderFast {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// InnerJoin adds a INNER JOIN clause to the query.
func (b SelectBuilderFast) InnerJoin(join string, rest ...interface{}) SelectBuilderFast {
	return b.JoinClause("INNER JOIN "+join, rest...)
}

// CrossJoin adds a CROSS JOIN clause to the query.
func (b SelectBuilderFast) CrossJoin(join string, rest ...interface{}) SelectBuilderFast {
	return b.JoinClause("CROSS JOIN "+join, rest...)
}

// Where adds an expression to the WHERE clause of the query.
//
// Expressions are ANDed together in the generated SQL.
//
// Where accepts several types for its pred argument:
//
// nil OR "" - ignored.
//
// string - SQL expression.
// If the expression has SQL placeholders then a set of arguments must be passed
// as well, one for each placeholder.
//
// map[string]interface{} OR Eq - map of SQL expressions to values. Each key is
// transformed into an expression like "<key> = ?", with the corresponding value
// bound to the placeholder. If the value is nil, the expression will be "<key>
// IS NULL". If the value is an array or slice, the expression will be "<key> IN
// (?,?,...)", with one placeholder for each item in the value. These expressions
// are ANDed together.
//
// Where will panic if pred isn't any of the above types.
func (b SelectBuilderFast) Where(pred interface{}, args ...interface{}) SelectBuilderFast {
	if pred == nil || pred == "" {
		return b
	}
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// GroupBy adds GROUP BY expressions to the query.
func (b SelectBuilderFast) GroupBy(groupBys ...string) SelectBuilderFast {
	b.groupBys = append(b.groupBys, groupBys...)
	return b
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b SelectBuilderFast) Having(pred interface{}, rest ...interface{}) SelectBuilderFast {
	b.havingParts = append(b.havingParts, newWherePart(pred, rest...))
	return b
}

// OrderByClause adds ORDER BY clause to the query.
func (b SelectBuilderFast) OrderByClause(pred interface{}, args ...interface{}) SelectBuilderFast {
	b.orderByParts = append(b.orderByParts, newPart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b SelectBuilderFast) OrderBy(orderBys ...string) SelectBuilderFast {
	for _, orderBy := range orderBys {
		b = b.OrderByClause(orderBy)
	}
	return b
}

// Limit sets a LIMIT clause on the query.
func (b SelectBuilderFast) Limit(limit uint64) SelectBuilderFast {
	b.limit = fmt.Sprintf("%d", limit)
	return b
}

// Limit ALL allows to access all records with limit
func (b SelectBuilderFast) RemoveLimit() SelectBuilderFast {
	b.limit = ""
	return b
}

// Offset sets a OFFSET clause on the query.
func (b SelectBuilderFast) Offset(offset uint64) SelectBuilderFast {
	b.offset = fmt.Sprintf("%d", offset)
	return b
}

// RemoveOffset removes OFFSET clause.
func (b SelectBuilderFast) RemoveOffset() SelectBuilderFast {
	b.offset = ""
	return b
}

// Suffix adds an expression to the end of the query
func (b SelectBuilderFast) Suffix(sql string, args ...interface{}) SelectBuilderFast {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b SelectBuilderFast) SuffixExpr(expr Sqlizer) SelectBuilderFast {
	b.suffixes = append(b.suffixes, expr)
	return b
}
