package queue

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestInitializeSchema(t *testing.T) {
	sqlitePath, err := TempFileName()
	if err != nil {
		t.Fatal(err)
	}

	testDb, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		t.Fatal(err)
	}

	err = InitializeSchema(testDb)
	if err != nil {
		t.Fatal(err)
	}

	if db == nil {
		t.Fatalf("db is nil.")
	}

	rows, _ := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table'`)

	defer rows.Close()

	tableName := ""
	tableCount := 0
	for rows.Next() {
		rows.Scan(&tableName)
		switch tableName {
		case "current_task", "tasks", "failed_tasks":
			tableCount += 1
			break
		}
	}

	if tableCount != 3 {
		t.Fatalf("failed create table!")
	}
}

func TestCloseDB(t *testing.T) {
	var err error

	sqlitePath, err := TempFileName()
	if err != nil {
		t.Fatal(err)
	}

	db, err = sql.Open("sqlite3", sqlitePath)
	if err != nil {
		t.Fatal(err)
	}

	CloseDB()

	if db.Stats().Idle != 0 {
		t.Fatalf("DB Idle connections exist!")
	}
}
