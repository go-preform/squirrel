package squirrel

import "github.com/lann/builder"

// StatementBuilderType is the type of StatementBuilder.
type StatementBuilderType builder.Builder

// Select returns a SelectBuilder for this StatementBuilderType.
func (b StatementBuilderType) Select(columns ...string) SelectBuilder {
	return SelectBuilder(b).Columns(columns...)
}
func (b StatementBuilderType) SelectFast(columns ...string) SelectBuilderFast {
	return SelectBuilderFast{placeholderFormat: b.getPlaceholderFormat(), columns: make([]Sqlizer, 0, 10), whereParts: make([]Sqlizer, 0, 10)}.Columns(columns...)
}

// Insert returns a InsertBuilder for this StatementBuilderType.
func (b StatementBuilderType) Insert(into string) InsertBuilder {
	return InsertBuilder(b).Into(into)
}
func (b StatementBuilderType) InsertFast(into string) InsertBuilderFast {
	return InsertBuilderFast{placeholderFormat: b.getPlaceholderFormat(), columns: make([]string, 0, 10), values: make([][]interface{}, 0, 10)}.Into(into)
}

// Replace returns a InsertBuilder for this StatementBuilderType with the
// statement keyword set to "REPLACE".
func (b StatementBuilderType) Replace(into string) InsertBuilder {
	return InsertBuilder(b).statementKeyword("REPLACE").Into(into)
}
func (b StatementBuilderType) ReplaceFast(into string) InsertBuilderFast {
	return InsertBuilderFast{placeholderFormat: b.getPlaceholderFormat(), columns: make([]string, 0, 10), values: make([][]interface{}, 0, 10)}.statementKeyword("REPLACE").Into(into)
}

// Update returns a UpdateBuilder for this StatementBuilderType.
func (b StatementBuilderType) Update(table string) UpdateBuilder {
	return UpdateBuilder(b).Table(table)
}
func (b StatementBuilderType) UpdateFast(table string) UpdateBuilderFast {
	return UpdateBuilderFast{placeholderFormat: b.getPlaceholderFormat(), setClauses: make([]setClause, 0, 10), whereParts: make([]Sqlizer, 0, 10)}.Table(table)
}

// Delete returns a DeleteBuilder for this StatementBuilderType.
func (b StatementBuilderType) Delete(from string) DeleteBuilder {
	return DeleteBuilder(b).From(from)
}
func (b StatementBuilderType) DeleteFast(from string) DeleteBuilderFast {
	return DeleteBuilderFast{placeholderFormat: b.getPlaceholderFormat(), whereParts: make([]Sqlizer, 0, 10)}.From(from)
}

// PlaceholderFormat sets the PlaceholderFormat field for any child builders.
func (b StatementBuilderType) PlaceholderFormat(f PlaceholderFormat) StatementBuilderType {
	return builder.Set(b, "PlaceholderFormat", f).(StatementBuilderType)
}

// RunWith sets the RunWith field for any child builders.
func (b StatementBuilderType) RunWith(runner BaseRunner) StatementBuilderType {
	return setRunWith(b, runner).(StatementBuilderType)
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b StatementBuilderType) Where(pred interface{}, args ...interface{}) StatementBuilderType {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(StatementBuilderType)
}

func (b StatementBuilderType) getPlaceholderFormat() PlaceholderFormat {
	if f, ok := builder.Get(b, "PlaceholderFormat"); ok {
		return f.(PlaceholderFormat)
	}
	return Question
}

// StatementBuilder is a parent builder for other builders, e.g. SelectBuilder.
var StatementBuilder = StatementBuilderType(builder.EmptyBuilder).PlaceholderFormat(Question)

// Select returns a new SelectBuilder, optionally setting some result columns.
//
// See SelectBuilder.Columns.
func Select(columns ...string) SelectBuilder {
	return StatementBuilder.Select(columns...)
}
func SelectFast(columns ...string) SelectBuilderFast {
	return StatementBuilder.SelectFast(columns...)
}

// Insert returns a new InsertBuilder with the given table name.
//
// See InsertBuilder.Into.
func Insert(into string) InsertBuilder {
	return StatementBuilder.Insert(into)
}

// InsertFast returns a new InsertBuilder with the given table name.
//
// See InsertBuilder.Into.
func InsertFast(into string) InsertBuilderFast {
	return StatementBuilder.InsertFast(into)
}

// Replace returns a new InsertBuilder with the statement keyword set to
// "REPLACE" and with the given table name.
//
// See InsertBuilder.Into.
func Replace(into string) InsertBuilder {
	return StatementBuilder.Replace(into)
}
func ReplaceFast(into string) InsertBuilderFast {
	return StatementBuilder.ReplaceFast(into)
}

// Update returns a new UpdateBuilder with the given table name.
//
// See UpdateBuilder.Table.
func Update(table string) UpdateBuilder {
	return StatementBuilder.Update(table)
}
func UpdateFast(table string) UpdateBuilderFast {
	return StatementBuilder.UpdateFast(table)
}

// Delete returns a new DeleteBuilder with the given table name.
//
// See DeleteBuilder.Table.
func Delete(from string) DeleteBuilder {
	return StatementBuilder.Delete(from)
}
func DeleteFast(from string) DeleteBuilderFast {
	return StatementBuilder.DeleteFast(from)
}

// Case returns a new CaseBuilder
// "what" represents case value
func Case(what ...interface{}) CaseBuilder {
	b := CaseBuilder(builder.EmptyBuilder)

	switch len(what) {
	case 0:
	case 1:
		b = b.what(what[0])
	default:
		b = b.what(newPart(what[0], what[1:]...))

	}
	return b
}

func CaseFast(what ...interface{}) CaseBuilderFast {
	b := CaseBuilderFast{}

	switch len(what) {
	case 0:
	case 1:
		b = b.what(what[0])
	default:
		b = b.what(newPart(what[0], what[1:]...))

	}
	return b
}
