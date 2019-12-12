package queue

import (
	"database/sql"
	"strconv"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func InitializeForTest(t *testing.T) {
	sqlitePath, err := TempFileName()
	if err != nil {
		t.Fatal(err)
	}

	testDb, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		t.Fatal(err)
	}

	InitializeSchema(testDb)

	youtubeDlPath = ""
	ffmpegPath = ""
}

func insertTaskForTest(t *testing.T, task Task) {
	task.SetId()

	if _, err := db.Exec(
		`INSERT INTO tasks (id, video_format, audio_format, url, title, output_path, parameter, created_at, updated_at, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		&task.Id,
		&task.VideoFormat,
		&task.AudioFormat,
		&task.Url,
		&task.Title,
		&task.OutputPath,
		&task.Parameter,
		&task.CreatedAt,
		&task.UpdatedAt,
		0,
	); err != nil {
		t.Fatal(err)
	}
}

func getTaskForTest(t *testing.T) Task {
	task := Task{}

	if err := db.QueryRow(`SELECT id, video_format, audio_format, title, created_at, updated_at FROM tasks ORDER BY id DESC LIMIT 1`).Scan(
		&task.Id,
		&task.VideoFormat,
		&task.AudioFormat,
		&task.Title,
		&task.CreatedAt,
		&task.UpdatedAt,
	); err != nil {
		t.Fatal(err)
	}

	return task
}

func getTaskByIdForTest(t *testing.T, id string, videoFormat string, audioFormat string) Task {
	task := Task{}

	if err := db.QueryRow(`SELECT id, video_format, audio_format, title, started_at FROM tasks WHERE id = ? AND video_format = ? AND audio_format = ?`, id, videoFormat, audioFormat).Scan(
		&task.Id,
		&task.VideoFormat,
		&task.AudioFormat,
		&task.Title,
		&task.StartedAt,
	); err != nil {
		t.Fatal(err)
	}

	return task
}

func TestExec(t *testing.T) {
	InitializeForTest(t)
}

func TestSetId(t *testing.T) {
	task := Task{
		Url: "https://www.youtube.com/watch?v=abcdefg",
	}

	err := task.SetId()
	if err != nil {
		t.Fatal(err)
	}

	if task.Id != "abcdefg" {
		t.Fatalf("Cannot extract Id!")
	}
}

func TestAddTask(t *testing.T) {
	InitializeForTest(t)

	task := Task{
		VideoFormat: "135",
		AudioFormat: "140",
		Url:         "https://www.youtube.com/watch?v=AddTask",
		Title:       "TestAddTask",
		OutputPath:  "/tmp/output",
		Parameter:   "",
	}

	err := task.AddTask()
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueueTask(t *testing.T) {
	InitializeForTest(t)

	task := Task{
		VideoFormat: "135",
		AudioFormat: "140",
		Url:         "https://www.youtube.com/watch?v=QueueTask",
		Title:       "TestQueueTask",
		OutputPath:  "/tmp/output",
		Parameter:   "",
	}

	err := task.QueueTask()
	if err != nil {
		t.Fatal(err)
	}

	if task.CreatedAt == 0 || task.UpdatedAt == 0 {
		t.Fatalf("Not set CreatedAt or UpdatedAt!")
	}
}

func TestStartTask(t *testing.T) {
	InitializeForTest(t)

	insertTaskForTest(t, Task{
		VideoFormat: "135",
		AudioFormat: "140",
		Url:         "https://www.youtube.com/watch?v=StartTask",
		Title:       "TestStartTask",
		OutputPath:  "/tmp/output",
		Parameter:   "",
		CreatedAt:   0,
		UpdatedAt:   0,
	})

	task := getTaskForTest(t)

	err := task.StartTask()
	if err != nil {
		t.Fatal(err)
	}

	targetTask := getTaskByIdForTest(t, task.Id, task.VideoFormat, task.AudioFormat)
	if targetTask.StartedAt == 0 {
		t.Fatalf("Not set StartedAt!")
	}
}

func TestFinishTask(t *testing.T) {
	InitializeForTest(t)

	insertTaskForTest(t, Task{
		VideoFormat: "135",
		AudioFormat: "140",
		Url:         "https://www.youtube.com/watch?v=FinishTask",
		Title:       "TestFinishTask",
		OutputPath:  "/tmp/output",
		Parameter:   "",
		CreatedAt:   0,
		UpdatedAt:   0,
	})

	task := getTaskForTest(t)

	err := task.FinishTask()
	if err != nil {
		t.Fatal(err)
	}

	id := int64(0)
	if err := db.QueryRow(`SELECT id FROM tasks WHERE id = ?`, task.Id).Scan(
		&id,
	); err == nil {
		t.Fatalf("Failed delete task!")
	}
}

func TestAddFailedTask(t *testing.T) {
	InitializeForTest(t)

	insertTaskForTest(t, Task{
		VideoFormat: "135",
		AudioFormat: "140",
		Url:         "https://www.youtube.com/watch?v=AddFailedTask",
		Title:       "TestAddFailedTask",
		OutputPath:  "/tmp/output",
		Parameter:   "",
		CreatedAt:   0,
		UpdatedAt:   0,
	})

	task := getTaskForTest(t)

	failedTask, err := task.AddFailedTask()
	if err != nil {
		t.Fatal(err)
	}

	if failedTask.FailedAt == 0 {
		t.Fatalf("Not set FailedAt!")
	}
}

func TestPopTasks(t *testing.T) {
	InitializeForTest(t)

	insertTaskForTest(t, Task{
		VideoFormat: "135",
		AudioFormat: "140",
		Url:         "https://www.youtube.com/watch?v=popTasks",
		Title:       "TestPopTasks",
		OutputPath:  "/tmp/output",
		Parameter:   "",
		CreatedAt:   0,
		UpdatedAt:   0,
	})

	tasks, err := popTasks()
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != workerNum {
		t.Fatalf("different tasks size!")
	}
}

func TestGetAllTasks(t *testing.T) {
	InitializeForTest(t)

	for i := 1; i <= 3; i++ {
		insertTaskForTest(t, Task{
			VideoFormat: "135",
			AudioFormat: "140",
			Url:         "https://www.youtube.com/watch?v=GetAllTasks" + strconv.Itoa(i),
			Title:       "TestGetAllTasks" + strconv.Itoa(i),
			OutputPath:  "/tmp/output",
			Parameter:   "",
			CreatedAt:   0,
			UpdatedAt:   0,
		})
	}

	tasks, err := GetAllTasks()
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 3 {
		t.Fatalf("different tasks size!")
	}
}
