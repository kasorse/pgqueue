package pgqueue

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
)

func setUp(t *testing.T) *sqlx.DB {
	cfgDatabaseDsn := os.Getenv("DATABASE_DSN")
	if cfgDatabaseDsn == "" {
		t.Fatal("To run the test, specify the DATABASE_DSN variable")
	}

	cc, err := pgx.ParseDSN(cfgDatabaseDsn)
	if err != nil {
		log.Fatalf("can't parse dsn. err: %s", err)
	}
	cc.PreferSimpleProtocol = true
	pdb := stdlib.OpenDB(cc)

	db := sqlx.NewDb(pdb, "pgx")
	db.SetMaxOpenConns(10)

	return db
}

func clean(t *testing.T, d *sqlx.DB) {
	if os.Getenv("CLEAN_ON_SETUP") == "disable" {
		return
	}

	var allTables []string

	err := d.Select(&allTables, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema='public'
		AND table_type='BASE TABLE'
		AND table_name NOT IN ('goose_db_version', 'queue_status');
	`)

	if err != nil {
		t.Fatal("Can't get list of tables", err)
	}

	q := bytes.NewBufferString("")
	for _, tbl := range allTables {
		q.WriteString(fmt.Sprintf("DELETE FROM %q;", tbl))
	}
	_, err = d.Exec(q.String())
	if err != nil {
		t.Fatal("Can't clean tables", err)
	}
}

func tearDown(d *sqlx.DB) {
	d.Close()
}
