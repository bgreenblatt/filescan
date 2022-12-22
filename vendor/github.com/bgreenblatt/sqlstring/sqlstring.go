// SqlString

// Copyright ©2022 Bruce Greenblatt

// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

package sqlstring

import (
	"fmt"
	"strconv"
	"strings"
)

type SQLString struct {
	buildsql        strings.Builder
	useDoubleQuotes bool
}

type SelectAllDistinctOption int

const (
	None SelectAllDistinctOption = iota
	All
	Distinct
)

func (s SelectAllDistinctOption) String() string {
	switch s {
	case None:
		return ""
	case All:
		return "ALL "
	case Distinct:
		return "DISTINCT "
	default:
		return ""
	}
}

type DefaultValue struct {
	Value     any
	UseQuotes bool
}

type SQLStringSelect struct {
	sqlString     SQLString
	columns       []string
	tables        []string
	where         string
	groupby       []string
	orderby       []string
	allOrDistinct SelectAllDistinctOption
	limit         uint
	offset        uint
}

type SelectJunctionOption int

const (
	Union SelectJunctionOption = iota
	UnionAll
	Intersect
	Except
)

func (s SelectJunctionOption) String() string {
	switch s {
	case Union:
		return " UNION "
	case UnionAll:
		return " ALL "
	case Intersect:
		return " DISTINCT "
	case Except:
		return " EXCEPT "
	default:
		return ""
	}
}

type SQLStringCompoundSelect struct {
	sqlString      SQLString
	sqlStringLeft  SQLStringSelect
	sqlStringRight SQLStringSelect
	junction       SelectJunctionOption
}

type SQLStringUpdate struct {
	sqlString SQLString
	columns   []string
	values    []string
	isquoted  []bool
	table     string
	where     string
}

type SQLStringInsert struct {
	sqlString      SQLString
	selectStmt     *SQLStringSelect
	columns        []string
	values         []string
	isquoted       []bool
	table          string
	conflictOption InsertConflictOption
}

type SQLStringDelete struct {
	sqlString SQLString
	table     string
	where     string
}

type ForeignKeyConstraint struct {
	srcColumns []string
	tgtColumns []string
	table      string
}

type SQLStringCreateTable struct {
	sqlString   SQLString
	table       string
	columns     []string
	ifNotExists bool
	foreignKeys []ForeignKeyConstraint
}

type SQLStringCreateIndex struct {
	sqlString   SQLString
	table       string
	index       string
	columns     []string
	ifNotExists bool
	isUnique    bool
	where       string
}

type TransactionType int

const (
	Begin TransactionType = iota
	Commit
	Rollback
)

func (t TransactionType) String() string {
	switch t {
	case Begin:
		return "BEGIN"
	case Commit:
		return "COMMIT"
	case Rollback:
		return "ROLLBACK"
	default:
		return ""
	}
}

type SQLStringTransaction struct {
	sqlString       SQLString
	transactionType TransactionType
}

func NewSQLStringTransaction(t TransactionType) *SQLStringTransaction {
	var stmt SQLStringTransaction

	stmt.transactionType = t
	return &stmt
}

func (t *SQLStringTransaction) Reset() {
	t.sqlString.Reset()
}

func (t *SQLStringTransaction) String() string {
	t.sqlString.Reset()
	t.sqlString.AddString(t.transactionType.String(), false)
	t.sqlString.AddString(" TRANSACTION", false)
	return t.sqlString.String()
}

func (t SQLString) String() string {
	return t.buildsql.String()
}

func (t *SQLString) AddString(s string, addComma bool) {
	t.buildsql.WriteString(s)
	if addComma {
		t.buildsql.WriteString(",")
	}
}

func (t *SQLString) AddStringWithQuotes(s string, addComma bool) {
	var quoteString string
	if t.useDoubleQuotes {
		quoteString = "\""
	} else {
		quoteString = "'"
	}
	t.buildsql.WriteString(quoteString)
	t.buildsql.WriteString(s)
	t.buildsql.WriteString(quoteString)
	if addComma {
		t.buildsql.WriteString(",")
	}
}

func (t *SQLString) AddStrings(s []string, sep string, addComma bool) {
	t.buildsql.WriteString(strings.Join(s, sep))
	if addComma {
		t.buildsql.WriteString(",")
	}
}

func (t *SQLString) AddStringsWithParens(s []string, sep string, addComma bool) {
	t.buildsql.WriteString("(")
	t.AddStrings(s, sep, false)
	t.buildsql.WriteString(")")
	if addComma {
		t.buildsql.WriteString(",")
	}
}

func (t *SQLString) AddStringWithParens(s string, addComma bool) {
	t.buildsql.WriteString("(")
	t.AddString(s, false)
	t.buildsql.WriteString(")")
	if addComma {
		t.buildsql.WriteString(",")
	}
}

func (t *SQLString) AddStringsWithQuotes(s []string, sep string, addComma bool) {
	var quoteString string
	if t.useDoubleQuotes {
		quoteString = "\""
	} else {
		quoteString = "'"
	}
	qSep := quoteString + sep + quoteString
	growSize := len(s) * (len(s[0]) + len(qSep))
	t.buildsql.Grow(growSize)
	t.buildsql.WriteString(quoteString)
	t.buildsql.WriteString(strings.Join(s, qSep))
	t.buildsql.WriteString(quoteString)
	if addComma {
		t.buildsql.WriteString(",")
	}
}

func (t *SQLString) AddInt(i int, addComma bool) {
	s := strconv.Itoa(i)
	t.buildsql.WriteString(s)
	if addComma {
		t.buildsql.WriteString(",")
	}
}

func (t *SQLString) AddUInt(i uint, addComma bool) {
	s := strconv.FormatUint(uint64(i), 10)
	t.buildsql.WriteString(s)
	if addComma {
		t.buildsql.WriteString(",")
	}
}

func (t *SQLString) Reset() {
	t.buildsql.Reset()
}

func NewSQLString(useDoubleQuotes bool) *SQLString {
	var stmt SQLString
	if useDoubleQuotes {
		stmt.useDoubleQuotes = true
	}
	return &stmt
}

func NewSQLStringSelect(useDoubleQuotes bool) *SQLStringSelect {
	var stmt SQLStringSelect
	if useDoubleQuotes {
		stmt.sqlString.useDoubleQuotes = true
	}
	return &stmt
}

func (t *SQLStringSelect) AddColumn(c string, addComma bool) {
	t.columns = append(t.columns, c)
}

func (t *SQLStringSelect) AddTable(tbl string, addComma bool) {
	t.tables = append(t.tables, tbl)
}

func (t *SQLStringSelect) AddWhere(w string, addComma bool) {
	t.where = w
}

func (t *SQLStringSelect) AddGroupBy(gb string, addComma bool) {
	t.groupby = append(t.groupby, gb)
}

func (t *SQLStringSelect) AddOrderBy(ob string, addComma bool) {
	t.orderby = append(t.orderby, ob)
}

func (t *SQLStringSelect) AddLimit(limit, offset uint, addComma bool) {
	t.limit = limit
	t.offset = offset
}

func (t *SQLStringSelect) AddAllDistinctOption(s SelectAllDistinctOption) {
	t.allOrDistinct = s
}

func (t *SQLStringSelect) String() string {
	t.sqlString.Reset()
	t.sqlString.AddString("SELECT ", false)
	t.sqlString.AddString(t.allOrDistinct.String(), false)
	columns := strings.Join(t.columns, ", ")
	t.sqlString.AddString(columns, false)
	tables := strings.Join(t.tables, ", ")
	t.sqlString.AddString(" FROM ", false)
	t.sqlString.AddString(tables, false)
	t.sqlString.AddString(" WHERE ", false)
	t.sqlString.AddString(t.where, false)
	if len(t.groupby) > 0 {
		gbs := strings.Join(t.groupby, ", ")
		t.sqlString.AddString(" GROUP BY ", false)
		t.sqlString.AddString(gbs, false)
	}
	if len(t.orderby) > 0 {
		obs := strings.Join(t.orderby, ", ")
		t.sqlString.AddString(" ORDER BY ", false)
		t.sqlString.AddString(obs, false)
	}
	if t.limit > 0 {
		t.sqlString.AddString(" LIMIT ", false)
		t.sqlString.AddUInt(t.limit, false)
		if t.offset > 0 {
			t.sqlString.AddString(" OFFSET ", false)
			t.sqlString.AddUInt(t.offset, false)
		}
	}
	return t.sqlString.String()
}

func (t *SQLStringSelect) Reset() {
	t.sqlString.Reset()
}

func NewSQLStringCompoundSelect(useDoubleQuotes bool) *SQLStringCompoundSelect {
	var stmt SQLStringCompoundSelect
	if useDoubleQuotes {
		stmt.sqlStringLeft.sqlString.useDoubleQuotes = true
		stmt.sqlStringRight.sqlString.useDoubleQuotes = true
	}
	return &stmt
}

func (t *SQLStringCompoundSelect) SetLeft(s SQLStringSelect) {
	t.sqlStringLeft = s
}

func (t *SQLStringCompoundSelect) SetRight(s SQLStringSelect) {
	t.sqlStringRight = s
}

func (t *SQLStringCompoundSelect) SetJunction(j SelectJunctionOption) {
	t.junction = j
}

func (t *SQLStringCompoundSelect) String() string {
	t.sqlString.Reset()
	t.sqlString.AddString(t.sqlStringLeft.String(), false)
	t.sqlString.AddString(t.junction.String(), false)
	t.sqlString.AddString(t.sqlStringRight.String(), false)
	return t.sqlString.String()
}

func (t *SQLStringCompoundSelect) Reset() {
	t.sqlString.Reset()
}

func NewSQLStringUpdate(useDoubleQuotes bool) *SQLStringUpdate {
	var stmt SQLStringUpdate
	if useDoubleQuotes {
		stmt.sqlString.useDoubleQuotes = true
	}
	return &stmt
}

func (t *SQLStringUpdate) AddColumnValue(c string, v string, q bool) {
	t.columns = append(t.columns, c)
	t.values = append(t.values, v)
	t.isquoted = append(t.isquoted, q)
}

func (t *SQLStringUpdate) AddTable(tbl string, addComma bool) {
	t.table = tbl
}

func (t *SQLStringUpdate) AddWhere(w string, addComma bool) {
	t.where = w
}

func (t *SQLStringUpdate) String() string {
	t.sqlString.Reset()
	t.sqlString.AddString("UPDATE ", false)
	t.sqlString.AddString(t.table, false)
	t.sqlString.AddString(" SET ", false)
	for i := range t.columns {
		t.sqlString.AddString(t.columns[i], false)
		t.sqlString.AddString(" = ", false)
		var addComma bool
		if i+1 == len(t.columns) {
			addComma = false
		} else {
			addComma = true
		}
		if t.isquoted[i] {
			t.sqlString.AddStringWithQuotes(t.values[i], addComma)
		} else {
			t.sqlString.AddString(t.values[i], addComma)
		}
		if i+1 < len(t.columns) {
			t.sqlString.AddString(" ", false)
		}
	}
	t.sqlString.AddString(" WHERE ", false)
	t.sqlString.AddString(t.where, false)
	return t.sqlString.String()
}

func (t *SQLStringUpdate) Reset() {
	t.sqlString.Reset()
}

type InsertConflictOption int

const (
	NoConflict InsertConflictOption = iota
	Replace
	Ignore
)

func (s InsertConflictOption) String() string {
	switch s {
	case NoConflict:
		return ""
	case Replace:
		return "OR REPLACE "
	case Ignore:
		return "OR IGNORE "
	default:
		return ""
	}
}
func (t *SQLStringInsert) AddConflictOption(s InsertConflictOption) {
	t.conflictOption = s
}

func NewSQLStringInsert(useDoubleQuotes bool) *SQLStringInsert {
	var stmt SQLStringInsert
	if useDoubleQuotes {
		stmt.sqlString.useDoubleQuotes = true
	}
	return &stmt
}

func (t *SQLStringInsert) AddColumnValue(c string, v string, q bool) {
	var quoteString string
	t.columns = append(t.columns, c)
	if !q {
		t.values = append(t.values, v)
	} else {
		if t.sqlString.useDoubleQuotes {
			quoteString = "\""
		} else {
			quoteString = "'"
		}
		qv := quoteString + v + quoteString
		t.values = append(t.values, qv)
	}
}

func (t *SQLStringInsert) AddTable(tbl string, addComma bool) {
	t.table = tbl
}

func (t *SQLStringInsert) AddSelect(s *SQLStringSelect) {
	t.selectStmt = s
}

func (t *SQLStringInsert) String() string {
	t.sqlString.Reset()
	t.sqlString.AddString("INSERT ", false)
	t.sqlString.AddString(t.conflictOption.String(), false)
	t.sqlString.AddString("INTO ", false)
	t.sqlString.AddString(t.table, false)
	t.sqlString.AddString(" ", false)
	if t.selectStmt != nil {
		t.sqlString.AddString(t.selectStmt.String(), false)
	} else {
		t.sqlString.AddStringsWithParens(t.columns, ", ", false)
		t.sqlString.AddString(" VALUES ", false)
		t.sqlString.AddStringsWithParens(t.values, ", ", false)
	}
	return t.sqlString.String()
}

func (t *SQLStringInsert) Reset() {
	t.sqlString.Reset()
}

func NewSQLStringDelete(useDoubleQuotes bool) *SQLStringDelete {
	var stmt SQLStringDelete
	if useDoubleQuotes {
		stmt.sqlString.useDoubleQuotes = true
	}
	return &stmt
}

func (t *SQLStringDelete) AddTable(tbl string, addComma bool) {
	t.table = tbl
}

func (t *SQLStringDelete) AddWhere(w string, addComma bool) {
	t.where = w
}

func (t *SQLStringDelete) String() string {
	t.sqlString.Reset()
	t.sqlString.AddString("DELETE FROM ", false)
	t.sqlString.AddString(t.table, false)
	t.sqlString.AddString(" ", false)
	if t.where != "" {
		t.sqlString.AddString(" WHERE ", false)
		t.sqlString.AddString(t.where, false)
	}
	return t.sqlString.String()
}

func (t *SQLStringDelete) Reset() {
	t.sqlString.Reset()
}

func NewSQLStringCreateTable(useDoubleQuotes, ifNotExists bool) *SQLStringCreateTable {
	var stmt SQLStringCreateTable
	if useDoubleQuotes {
		stmt.sqlString.useDoubleQuotes = true
	}
	stmt.ifNotExists = ifNotExists
	return &stmt
}

func (t *SQLStringCreateTable) AddTable(tbl string, addComma bool) {
	t.table = tbl
}

func (t *SQLStringCreateTable) AddColumn(cn string, tv string, pk bool, dv *DefaultValue) {
	var colString SQLString

	colString.AddString(cn, false)
	colString.AddString(" ", false)
	colString.AddString(tv, false)
	if pk {
		colString.AddString(" PRIMARY KEY ", false)
	} else if dv != nil {
		colString.AddString(" DEFAULT ", false)
		v := fmt.Sprintf("%v", dv.Value)
		if dv.UseQuotes {
			colString.AddStringWithQuotes(v, false)
		} else {
			colString.AddString(v, false)
		}
	}
	t.columns = append(t.columns, colString.String())
}

func (t *SQLStringCreateTable) AddForeignKeyConstraint(srcColumns, tgtColumns []string, table string) {
	var fk ForeignKeyConstraint

	fmt.Printf("foreign key params: %v, %v, %s\n", srcColumns, tgtColumns, table)
	for _, src := range srcColumns {
		fk.srcColumns = append(fk.srcColumns, src)
	}
	for _, tgt := range tgtColumns {
		fk.tgtColumns = append(fk.tgtColumns, tgt)
	}
	fk.table = table
	fmt.Printf("Adding foreign key: %v\n", fk)
	t.foreignKeys = append(t.foreignKeys, fk)
}

func (t *SQLStringCreateTable) String() string {
	t.sqlString.Reset()
	t.sqlString.AddString("CREATE TABLE ", false)
	if t.ifNotExists {
		t.sqlString.AddString("IF NOT EXISTS ", false)
	}
	t.sqlString.AddString(t.table, false)
	if len(t.foreignKeys) == 0 {
		t.sqlString.AddStringsWithParens(t.columns, ", ", false)
	} else {
		t.sqlString.AddString("(", false)
		t.sqlString.AddStrings(t.columns, ", ", false)
		for _, fk := range t.foreignKeys {
			t.sqlString.AddString(", FOREIGN KEY ", false)
			t.sqlString.AddStringsWithParens(fk.srcColumns, ", ", false)
			t.sqlString.AddString(" REFERENCES ", false)
			t.sqlString.AddString(fk.table, false)
			t.sqlString.AddStringsWithParens(fk.tgtColumns, ", ", false)
		}
		t.sqlString.AddString(")", false)
	}
	return t.sqlString.String()
}

func (t *SQLStringCreateTable) Reset() {
	t.sqlString.Reset()
}

func NewSQLStringCreateIndex(useDoubleQuotes, ifNotExists, isUnique bool) *SQLStringCreateIndex {
	var stmt SQLStringCreateIndex
	if useDoubleQuotes {
		stmt.sqlString.useDoubleQuotes = true
	}
	stmt.ifNotExists = ifNotExists
	stmt.isUnique = isUnique
	return &stmt
}

func (t *SQLStringCreateIndex) AddTable(tbl string, addComma bool) {
	t.table = tbl
}

func (t *SQLStringCreateIndex) AddIndex(idx string, addComma bool) {
	t.index = idx
}

func (t *SQLStringCreateIndex) AddColumn(cn string) {
	t.columns = append(t.columns, cn)
}

func (t *SQLStringCreateIndex) AddWhere(w string, addComma bool) {
	t.where = w
}

func (t *SQLStringCreateIndex) String() string {
	t.sqlString.Reset()
	if t.isUnique {
		t.sqlString.AddString("CREATE UNIQUE INDEX ", false)
	} else {
		t.sqlString.AddString("CREATE INDEX ", false)
	}
	if t.ifNotExists {
		t.sqlString.AddString("IF NOT EXISTS ", false)
	}
	t.sqlString.AddString(t.index, false)
	t.sqlString.AddString(" ON ", false)
	t.sqlString.AddString(t.table, false)
	t.sqlString.AddStringsWithParens(t.columns, ", ", false)
	if t.where != "" {
		t.sqlString.AddString(" WHERE ", false)
		t.sqlString.AddString(t.where, false)
	}
	return t.sqlString.String()
}

func (t *SQLStringCreateIndex) Reset() {
	t.sqlString.Reset()
}
