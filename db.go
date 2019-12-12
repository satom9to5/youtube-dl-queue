package queue

import (
	"database/sql"
	"errors"
)

var (
	db       *sql.DB
	sqlStmts = make(map[string]*sql.Stmt)
)

func InitializeSchema(varDB *sql.DB) (err error) {
	if err = initializeDB(varDB); err != nil {
		return err
	}

	sqls := []string{
		`CREATE TABLE IF NOT EXISTS "current_task" (
	    "id" TEXT NOT NULL,	
	    "video_format" TEXT NOT NULL,	
	    "audio_format" TEXT NOT NULL,	
			PRIMARY KEY ("id", "video_format", "audio_format")
		)`,
		`CREATE TABLE IF NOT EXISTS "tasks" (
	    "id" TEXT NOT NULL,	
	    "video_format" TEXT NOT NULL,	
	    "audio_format" TEXT NOT NULL,	
	    "url" TEXT NOT NULL,	
	    "title" TEXT NOT NULL,	
	    "output_path" TEXT NOT NULL,	
	    "parameter" TEXT NOT NULL,	
			"created_at" INTEGER NOT NULL,
			"updated_at" INTEGER NOT NULL,
			"started_at" INTEGER NOT NULL,
			PRIMARY KEY ("id", "video_format", "audio_format")
		)`,
		`CREATE TABLE IF NOT EXISTS "failed_tasks" (
	    "id" TEXT NOT NULL,	
	    "video_format" TEXT NOT NULL,	
	    "audio_format" TEXT NOT NULL,	
	    "url" TEXT NOT NULL,	
	    "title" TEXT NOT NULL,	
	    "output_path" TEXT NOT NULL,	
	    "parameter" TEXT NOT NULL,	
			"created_at" INTEGER NOT NULL,
			"updated_at" INTEGER NOT NULL,
			"started_at" INTEGER NOT NULL,
			"failed_at" INTEGER NOT NULL,
			PRIMARY KEY ("id", "video_format", "audio_format")
		)`,
	}

	for _, sql := range sqls {
		if _, err = db.Exec(sql); err != nil {
			return err
		}
	}

	return err
}

func CloseDB() {
	db.Close()
}

func initializeDB(varDB *sql.DB) error {
	db = varDB
	if db == nil {
		return errors.New("cannot found db!")
	}
	return nil
}

func createSqlStmt(sql string) (stmt *sql.Stmt, err error) {
	stmt, ok := sqlStmts[sql]
	if ok {
		return stmt, err
	}

	stmt, err = db.Prepare(sql)
	if err == nil {
		sqlStmts[sql] = stmt
		return stmt, err
	} else {
		return nil, err
	}
}
