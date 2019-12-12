package queue

import (
	"database/sql"
	"io/ioutil"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TempFileName() (string, error) {
	if f, err := ioutil.TempFile("", "youtube-dl-queue-test-"); err == nil {
		f.Close()
		return f.Name(), nil
	} else {
		return "", err
	}
}

func TestStartAndStop(t *testing.T) {
	sqlitePath, err := TempFileName()
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		t.Fatal(err)
	}

	pidfilePath, err := TempFileName()
	if err != nil {
		t.Fatal(err)
	}

	dispatchFlag := false
	dispatch = func() error { dispatchFlag = true; return nil }

	pid, err := Start(
		db,
		pidfilePath,
		"/tmp/youtubel-dl",
		"/tmp/ffmpeg",
	)

	if err != nil {
		t.Fatal(err)
	}

	// wait running dispatch
	time.Sleep(time.Second)

	if pid == 0 {
		t.Fatalf("pid is zero.")
	}

	if starting == false {
		t.Fatalf("starting flag is false.")
	}

	if dispatchFlag == false {
		t.Fatalf("not start dispatch.")
	}

	Stop()

	if starting == true {
		t.Fatalf("starting flag is true.")
	}
}
