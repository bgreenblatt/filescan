# sqlstring
Helper library for building SQL query strings in Go

Notes:

1. This library uses strings.Builder since it is much faster than concatenating strings with "+".
1. Current version includes support for the following basic SQL queries:
    * SELECT (including compound SELECTs like UNION)
    * UPDATE
    * INSERT (including REPLACE or IGNORE options)
    * DELETE
    * CREATE TABLE
    * CREATE INDEX
    * BEGIN and COMMIT TRANSACTION
1. There is also support for convenience functions for building strings so that arbitrary SQL queries can be created
1. Test code uses the sqlite3 command line tool for SQL statement validation

Install:

To use the package, just use the standard go get method:

```
go get github.com/bgreenblatt/sqlstring
```

Usage:

Building up strings for SQL queries can be done as in the following code segment:

```go
	var stmt SQLStringSelect

	stmt.AddColumn("c1", false)
	stmt.AddColumn("c2", false)
	stmt.AddTable("t2", false)
	stmt.AddWhere("c2 == 'ID2'", false)
	stmt.AddOrderBy("c2", false)
	stmt.AddGroupBy("c2", false)
	stmt.AddLimit(10, 0, false)
```

Then the result of `stmt.String()` would yield this query string:

```
SELECT c1, c2 FROM t2 WHERE c2 == 'ID2' GROUP BY c2 ORDER BY c2 LIMIT 10
```

If for some reason your SQL engine wants double quotes instead of single quotes
around embedded text strings for data values, then don't use the default constructor
for SQLString. Instead call `NewSQLString` and pass in true for the `useDoubleQuotes`
parameter like this:

```go
	stmt := NewSQLString(true)

	stmt.AddColumn("c1", false)
	stmt.AddColumn("c2", false)
	stmt.AddTable("t2", false)
	stmt.AddWhere("c2 == 'ID2'", false)
	stmt.AddOrderBy("c2", false)
	stmt.AddGroupBy("c2", false)
	stmt.AddLimit(10, 0, false)
```

Then the result of `stmt.String()` would yield this query string:

```
SELECT c1, c2 FROM t2 WHERE c2 == "ID2" GROUP BY c2 ORDER BY c2 LIMIT 10
```

Other queries can be built similarly. For example to build DELETE statement, the
following code can be used:

```go
	var stmt SQLStringDelete

	stmt.AddTable("t2", false)
	stmt.AddWhere("c2 = 'ID2'", false)
```

Then the result of `stmt.String()` would yield this query string:

```
DELETE FROM t2  WHERE c2 = 'ID2'
```
