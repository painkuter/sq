package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"squirrel/builder"
)

type deleteData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          exprs
	From              string
	WhereParts        []Sqlizer
	OrderBys          []string
	Limit             string
	Offset            string
	Suffixes          exprs
}

func (d *deleteData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *deleteData) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.From) == 0 {
		err = fmt.Errorf("delete statements must specify a From table")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, _ = d.Prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("DELETE FROM ")
	sql.WriteString(d.From)

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSql(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.OrderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(d.OrderBys, ", "))
	}

	if len(d.Limit) > 0 {
		sql.WriteString(" LIMIT ")
		sql.WriteString(d.Limit)
	}

	if len(d.Offset) > 0 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(d.Offset)
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = d.Suffixes.AppendToSql(sql, " ", args)
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

// Builder

// DeleteBuilder builds SQL DELETE statements.
type DeleteBuilder builder.Builder

func init() {
	builder.Register(DeleteBuilder{}, deleteData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *DeleteBuilder) PlaceholderFormat(f PlaceholderFormat) *DeleteBuilder {
	*b = builder.Set(*b, "PlaceholderFormat", f).(DeleteBuilder)
	return b
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *DeleteBuilder) RunWith(runner BaseRunner) *DeleteBuilder {
	*b = setRunWith(*b, runner).(DeleteBuilder)
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *DeleteBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(*b).(deleteData)
	return data.Exec()
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b *DeleteBuilder) ToSql() (string, []interface{}, error) {
	a := *b
	data := builder.GetStruct(a).(deleteData)
	return data.ToSql()
}

// Prefix adds an expression to the beginning of the query
func (b *DeleteBuilder) Prefix(sql string, args ...interface{}) *DeleteBuilder {
	*b = builder.Append(*b, "Prefixes", Expr(sql, args...)).(DeleteBuilder)
	return b
}

// From sets the table to be deleted from.
func (b *DeleteBuilder) From(from string) *DeleteBuilder {
	*b = builder.Set(*b, "From", from).(DeleteBuilder)
	return b
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b *DeleteBuilder) Where(pred interface{}, args ...interface{}) *DeleteBuilder {
	*b = builder.Append(*b, "WhereParts", newWherePart(pred, args...)).(DeleteBuilder)
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b *DeleteBuilder) OrderBy(orderBys ...string) *DeleteBuilder {
	*b = builder.Extend(*b, "OrderBys", orderBys).(DeleteBuilder)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *DeleteBuilder) Limit(limit uint64) *DeleteBuilder {
	*b = builder.Set(*b, "Limit", fmt.Sprintf("%d", limit)).(DeleteBuilder)
	return b
}

// Offset sets a OFFSET clause on the query.
func (b *DeleteBuilder) Offset(offset uint64) *DeleteBuilder {
	*b = builder.Set(*b, "Offset", fmt.Sprintf("%d", offset)).(DeleteBuilder)
	return b
}

// Suffix adds an expression to the end of the query
func (b *DeleteBuilder) Suffix(sql string, args ...interface{}) *DeleteBuilder {
	*b = builder.Append(*b, "Suffixes", Expr(sql, args...)).(DeleteBuilder)
	return b
}
