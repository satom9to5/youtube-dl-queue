package queue

import (
	"strconv"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func insertFailedTaskForTest(t *testing.T, failedTask FailedTask) {
	if _, err := db.Exec(
		`INSERT INTO failed_tasks (id, url, title, video_format, audio_format, output_path, parameter, created_at, updated_at, started_at, failed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		&failedTask.Id,
		&failedTask.Url,
		&failedTask.Title,
		&failedTask.VideoFormat,
		&failedTask.AudioFormat,
		&failedTask.OutputPath,
		&failedTask.Parameter,
		&failedTask.CreatedAt,
		&failedTask.UpdatedAt,
		&failedTask.StartedAt,
		&failedTask.FailedAt,
	); err != nil {
		t.Fatal(err)
	}
}

func getFailedTaskForTest(t *testing.T) FailedTask {
	failedTask := FailedTask{}

	if err := db.QueryRow(`SELECT id, video_format, audio_format, title, created_at, updated_at FROM failed_tasks ORDER BY id DESC LIMIT 1`).Scan(
		&failedTask.Id,
		&failedTask.VideoFormat,
		&failedTask.AudioFormat,
		&failedTask.Title,
		&failedTask.CreatedAt,
		&failedTask.UpdatedAt,
	); err != nil {
		t.Fatal(err)
	}

	return failedTask
}

func TestRequeueTask(t *testing.T) {
	InitializeForTest(t)

	insertFailedTaskForTest(t, FailedTask{
		Id:          "RequeueTask",
		VideoFormat: "135",
		AudioFormat: "140",
		Url:         "https://www.youtube.com/watch?v=RequeueTask",
		Title:       "TestRequeueTask",
		OutputPath:  "/tmp/output",
		Parameter:   "",
		CreatedAt:   0,
		UpdatedAt:   0,
		StartedAt:   0,
		FailedAt:    0,
	})

	failedTask := getFailedTaskForTest(t)

	task, err := failedTask.RequeueTask()
	if err != nil {
		t.Fatal(err)
	}

	if task.Title != failedTask.Title {
		t.Fatalf("failed insert task!")
	}

	if err := db.QueryRow(`SELECT id FROM failed_tasks ORDER BY id DESC LIMIT 1`).Scan(
		&failedTask.Id,
	); err == nil {
		t.Fatalf("failed delete failed_tasks!")
	}
}

func TestGetAllFailedTasks(t *testing.T) {
	InitializeForTest(t)

	for i := 1; i <= 3; i++ {
		insertFailedTaskForTest(t, FailedTask{
			Id:          "GetAllFailedTasks" + strconv.Itoa(i),
			VideoFormat: "135",
			AudioFormat: "140",
			Url:         "https://www.youtube.com/watch?v=GetAllFailedTasks" + strconv.Itoa(i),
			Title:       "TestGetAllFailedTasks" + strconv.Itoa(i),
			OutputPath:  "/tmp/output",
			Parameter:   "",
			CreatedAt:   0,
			UpdatedAt:   0,
			StartedAt:   0,
			FailedAt:    0,
		})
	}

	failedTasks, err := GetAllFailedTasks()
	if err != nil {
		t.Fatal(err)
	}

	if len(failedTasks) != 3 {
		t.Fatalf("different tasks size!")
	}
}
