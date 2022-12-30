package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/bgreenblatt/sqlstring"
	_ "github.com/mattn/go-sqlite3"
)

var rootPath = flag.String("path", "", "root path name")
var dbName = flag.String("dbName", "", "database name")
var makeDB = flag.Bool("makedb", false, "initiallize the db")
var report = flag.Bool("report", false, "run usage report")

var dirid, fileid int

func scanPath(rootPath string, parentid int, db *sql.DB) error {
	// fmt.Printf("now scanning %s\n", rootPath)
	dirlist := make(map[string]int)
	f, err := os.Open(rootPath)
	if err != nil {
		fmt.Println(err)
		return err
	}
	files, err := f.Readdir(0)
	if err != nil {
		fmt.Println(err)
		return err
	}

	for _, v := range files {
		if v.IsDir() {
			dirid += 1
			dirlist[v.Name()] = dirid
			if err := insertDirRecord(rootPath, v.Name(), dirid, parentid, db); err != nil {
				return err
			}
		} else {
			fileid += 1
			if err := insertFileRecord(rootPath, v, fileid, parentid, db); err != nil {
				return err
			}
			// fmt.Println(v.Name(), v.IsDir())
		}
	}
	for dirName, dirid := range dirlist {
		fullDir := filepath.Join(rootPath, dirName)
		if err := scanPath(fullDir, dirid, db); err != nil {
			return err
		}
	}
	return nil
}

func insertDirRecord(parentPath, dirName string, newDirid, parentDirid int, db *sql.DB) error {
	stmt := sqlstring.NewSQLStringInsert(true)

	stmt.AddTable("dirs", false)
	stmt.AddColumnValue("dirid", strconv.Itoa(newDirid), true)
	stmt.AddColumnValue("dirname", dirName, true)
	fullDir := filepath.Join(parentPath, dirName)
	stmt.AddColumnValue("fulldirname", fullDir, true)
	stmt.AddColumnValue("parentdirid", strconv.Itoa(parentDirid), true)
	stmt.AddConflictOption(sqlstring.Replace)
	_, err := db.Exec(stmt.String())
	if err != nil {
		fmt.Printf("err %v inserting record %s\n", err, stmt.String())
	}
	return err
}

func insertFileRecord(parentPath string, i os.FileInfo, newFileid, parentDirid int, db *sql.DB) error {
	stmt := sqlstring.NewSQLStringInsert(true)

	fileName := i.Name()
	stmt.AddTable("files", false)
	stmt.AddColumnValue("fileid", strconv.Itoa(newFileid), true)
	stmt.AddColumnValue("filename", fileName, true)
	fullFile := filepath.Join(parentPath, fileName)
	stat := i.Sys().(*syscall.Stat_t)
	uid := stat.Uid
	gid := stat.Gid
	stmt.AddColumnValue("fullfilename", fullFile, true)
	stmt.AddColumnValue("parentdirid", strconv.Itoa(parentDirid), true)
	stmt.AddColumnValue("filesize", strconv.FormatInt(i.Size(), 10), true)
	stmt.AddColumnValue("filemode", strconv.Itoa(int(i.Mode())), true)
	stmt.AddColumnValue("fileuid", strconv.Itoa(int(uid)), true)
	stmt.AddColumnValue("filegid", strconv.Itoa(int(gid)), true)
	mtime := int(i.ModTime().Unix())
	stmt.AddColumnValue("filemtime", strconv.Itoa(mtime), true)
	stmt.AddConflictOption(sqlstring.Replace)
	_, err := db.Exec(stmt.String())
	if err != nil {
		fmt.Printf("err %v inserting record %s\n", err, stmt.String())
	}
	return err
}

func createDB(dbName string) {
	srcColumns := []string{"parentdirid"}
	tgtColumns := []string{"dirid"}
	dirStmt := sqlstring.NewSQLStringCreateTable(true, true)
	dirStmt.AddTable("dirs", false)
	dirStmt.AddColumn("dirid", "INTEGER", true, nil)
	dirStmt.AddColumn("dirname", "TEXT", false, nil)
	dirStmt.AddColumn("fulldirname", "TEXT", false, nil)
	dirStmt.AddColumn("parentdirid", "INTEGER", false, nil)
	dirStmt.AddForeignKeyConstraint(srcColumns, tgtColumns, "dirs")
	fileStmt := sqlstring.NewSQLStringCreateTable(true, true)
	fileStmt.AddTable("files", false)
	fileStmt.AddColumn("fileid", "INTEGER", true, nil)
	fileStmt.AddColumn("filename", "TEXT", false, nil)
	fileStmt.AddColumn("fullfilename", "TEXT", false, nil)
	fileStmt.AddColumn("parentdirid", "INTEGER", false, nil)
	fileStmt.AddColumn("filesize", "INTEGER", false, nil)
	fileStmt.AddColumn("filemode", "INTEGER", false, nil)
	fileStmt.AddColumn("filemtime", "TIMESTAMP", false, nil)
	fileStmt.AddColumn("fileuid", "INTEGER", false, nil)
	fileStmt.AddColumn("filegid", "INTEGER", false, nil)
	fileStmt.AddForeignKeyConstraint(srcColumns, tgtColumns, "dirs")
	fmt.Printf("creating db %s\n", dbName)
	fmt.Printf("files table schema is:\n\t%s\n\t%s\n", dirStmt.String(), fileStmt.String())

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
	_, err = db.Exec(dirStmt.String())
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(fileStmt.String())
	if err != nil {
		log.Fatal(err)
	}
}

func runFileCountReport(db *sql.DB) error {
	var stmt sqlstring.SQLStringSelect
	stmt.AddColumn("d.fulldirname", false)
	stmt.AddColumn("count(f.fileid) as count", false)
	stmt.AddColumn("sum(f.filesize)", false)
	stmt.AddTable("files as f", false)
	stmt.AddTable("dirs as d", false)
	stmt.AddWhere("d.dirid = f.parentdirid", false)
	stmt.AddOrderBy("count", sqlstring.Descending)
	stmt.AddGroupBy("f.parentdirid", false)
	stmt.AddLimit(10, 0, false)

	rows, err := db.Query(stmt.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "\nFile Count Report\n")
	fmt.Fprintf(w, "\n %s\t%s\t%s\t", "Dirname", "Count of Files", "Sum of File Sizes")
	fmt.Fprintf(w, "\n %s\t%s\t%s\t", "----", "----", "----")
	for rows.Next() {
		var dirname string
		var sum, count int
		err = rows.Scan(&dirname, &count, &sum)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "\n %s\t%d\t%d\t", dirname, count, sum)
	}
	fmt.Fprintf(w, "\n")
	w.Flush()
	return nil
}

func runFileSizeReport(db *sql.DB) error {
	var stmt sqlstring.SQLStringSelect
	stmt.AddColumn("d.fulldirname", false)
	stmt.AddColumn("count(f.fileid)", false)
	stmt.AddColumn("sum(f.filesize)", false)
	stmt.AddTable("files as f", false)
	stmt.AddTable("dirs as d", false)
	stmt.AddWhere("d.dirid = f.parentdirid", false)
	stmt.AddOrderBy("filesize", sqlstring.Descending)
	stmt.AddGroupBy("f.parentdirid", false)
	stmt.AddLimit(10, 0, false)

	rows, err := db.Query(stmt.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "\nFile Size Report\n")
	fmt.Fprintf(w, "\n %s\t%s\t%s\t", "Dirname", "Count of Files", "Sum of File Sizes")
	fmt.Fprintf(w, "\n %s\t%s\t%s\t", "----", "----", "----")
	for rows.Next() {
		var dirname string
		var sum, count int
		err = rows.Scan(&dirname, &count, &sum)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "\n %s\t%d\t%d\t", dirname, count, sum)
	}
	fmt.Fprintf(w, "\n")
	w.Flush()
	return nil
}

func runFileUidReport(db *sql.DB) error {
	var stmt sqlstring.SQLStringSelect
	stmt.AddColumn("f.fileuid", false)
	stmt.AddColumn("count(f.fileid) as count", false)
	stmt.AddColumn("sum(f.filesize)", false)
	stmt.AddTable("files as f", false)
	stmt.AddTable("dirs as d", false)
	stmt.AddWhere("d.dirid = f.parentdirid", false)
	stmt.AddOrderBy("count", sqlstring.Descending)
	stmt.AddGroupBy("f.fileuid", false)
	stmt.AddLimit(10, 0, false)

	rows, err := db.Query(stmt.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "\nFile Count Report\n")
	fmt.Fprintf(w, "\n %s\t%s\t%s\t", "UID", "Count of Files", "Sum of File Sizes")
	fmt.Fprintf(w, "\n %s\t%s\t%s\t", "----", "----", "----")
	for rows.Next() {
		var dirname string
		var sum, count int
		err = rows.Scan(&dirname, &count, &sum)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "\n %s\t%d\t%d\t", dirname, count, sum)
	}
	fmt.Fprintf(w, "\n")
	w.Flush()
	return nil
}

// note date1 is the more recent date and date2 is the older one
func runFileAgingReportByDate(db *sql.DB, date1, date2 string) error {
	var stmt sqlstring.SQLStringSelect
	stmt.AddColumn("count(f.fileid) as count", false)
	stmt.AddColumn("sum(f.filesize)", false)
	stmt.AddTable("files as f", false)
	where1 := "f.filemtime < " + date1
	where2 := "f.filemtime > " + date2
	whereCombined := where1 + " AND " + where2
	stmt.AddWhere(whereCombined, false)
	stmt.AddLimit(10, 0, false)
	fmt.Printf("file aging query: %s\n", stmt.String())

	rows, err := db.Query(stmt.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "\nFile Aging Report (%s - %s)\n", date1, date2)
	fmt.Fprintf(w, "\n %s\t%s\t", "Count of Files", "Sum of File Sizes")
	fmt.Fprintf(w, "\n %s\t%s\t", "----", "----")
	for rows.Next() {
		var sum, count int
		err = rows.Scan(&count, &sum)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "\n %d\t%d\t", count, sum)
	}
	fmt.Fprintf(w, "\n")
	w.Flush()
	return nil
}

func runFileGidReport(db *sql.DB) error {
	var stmt sqlstring.SQLStringSelect
	stmt.AddColumn("f.filegid", false)
	stmt.AddColumn("count(f.fileid) as count", false)
	stmt.AddColumn("sum(f.filesize)", false)
	stmt.AddTable("files as f", false)
	stmt.AddTable("dirs as d", false)
	stmt.AddWhere("d.dirid = f.parentdirid", false)
	stmt.AddOrderBy("count", sqlstring.Descending)
	stmt.AddGroupBy("f.fileuid", false)
	stmt.AddLimit(10, 0, false)

	rows, err := db.Query(stmt.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "\nFile Count Report\n")
	fmt.Fprintf(w, "\n %s\t%s\t%s\t", "UID", "Count of Files", "Sum of File Sizes")
	fmt.Fprintf(w, "\n %s\t%s\t%s\t", "----", "----", "----")
	for rows.Next() {
		var dirname string
		var sum, count int
		err = rows.Scan(&dirname, &count, &sum)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "\n %s\t%d\t%d\t", dirname, count, sum)
	}
	fmt.Fprintf(w, "\n")
	w.Flush()
	return nil
}

func runReports(db *sql.DB) error {
	if err := runFileCountReport(db); err != nil {
		return err
	}
	if err := runFileSizeReport(db); err != nil {
		return err
	}
	if err := runFileUidReport(db); err != nil {
		return err
	}
	if err := runFileGidReport(db); err != nil {
		return err
	}
	t1 := time.Now()
	t2 := t1.AddDate(0, 0, -10)
	date1 := strconv.Itoa(int(t1.Unix()))
	date2 := strconv.Itoa(int(t2.Unix()))
	if err := runFileAgingReportByDate(db, date1, date2); err != nil {
		return err
	}
	return nil
}

func main() {
	start := time.Now()
	flag.Parse()
	if strings.Compare(*dbName, "") == 0 {
		fmt.Printf("Must supply db name to use --path\n")
		os.Exit(3)
	}
	if *makeDB {
		createDB(*dbName)
		os.Exit(0)
	}

	db, err := sql.Open("sqlite3", *dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if *report {
		if err := runReports(db); err != nil {
			fmt.Printf("error %v running report\n", err)
			os.Exit(3)
		}
		os.Exit(0)
	}
	if strings.Compare(*rootPath, "") == 0 {
		fmt.Printf("Must supply root path name to traverse --path\n")
		os.Exit(3)
	}

	beginStmt := sqlstring.NewSQLStringTransaction(sqlstring.Begin)
	_, err = db.Exec(beginStmt.String())
	if err != nil {
		fmt.Printf("err %v beginning transaction\n", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func(ctx context.Context) {
		for {
			time.Sleep(1000 * time.Millisecond)
			select {
			case <-ctx.Done(): // if cancel() execute
				return
			default:
				fmt.Printf("Inserted %d files and %d dirs\n", fileid, dirid)
			}

		}
	}(ctx)
	err = scanPath(*rootPath, 0, db)
	fmt.Printf("err %v scanning %s\n", err, *rootPath)
	commitStmt := sqlstring.NewSQLStringTransaction(sqlstring.Commit)
	_, err = db.Exec(commitStmt.String())
	if err != nil {
		fmt.Printf("err %v committing transaction\n", err)
	}
	dur := time.Since(start)
	fmt.Printf("Inserted %d files and %d dirs in %s\n", fileid, dirid, dur.String())
}
