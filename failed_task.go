package queue

import (
	"fmt"
	"time"
)

type FailedTask struct {
	Id          string `json:"id"`
	VideoFormat string `json:"video_format"`
	AudioFormat string `json:"audio_format"`
	Url         string `json:"url"`
	Title       string `json:"title"`
	OutputPath  string `json:"output_path"`
	Parameter   string `json:"parameter"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
	StartedAt   int64  `json:"started_at"`
	FailedAt    int64  `json:"failed_at"`
}

func (ft FailedTask) String() string {
	return fmt.Sprintf(
		"Id: %s\tVideoFormat:%s\tAudioFormat:%s\tUrl:%s\tTitle:%s\tOutputPath:%s\tParameter:%s",
		ft.Id,
		ft.VideoFormat,
		ft.AudioFormat,
		ft.Url,
		ft.Title,
		ft.OutputPath,
		ft.Parameter,
	)
}

func (ft *FailedTask) AddTask() error {
	stmt, err := createSqlStmt(`INSERT INTO failed_tasks (id, video_format, audio_format, url, title, output_path, parameter, created_at, updated_at, started_at, failed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	ft.FailedAt = time.Now().Unix()

	_, err = stmt.Exec(
		ft.Id,
		ft.VideoFormat,
		ft.AudioFormat,
		ft.OutputPath,
		ft.Url,
		ft.Title,
		ft.Parameter,
		ft.CreatedAt,
		ft.UpdatedAt,
		ft.StartedAt,
		ft.FailedAt,
	)

	return err
}

func (ft *FailedTask) RequeueTask() (task Task, err error) {
	task = Task{
		Id:          ft.Id,
		VideoFormat: ft.VideoFormat,
		AudioFormat: ft.AudioFormat,
		Url:         ft.Url,
		Title:       ft.Title,
		OutputPath:  ft.OutputPath,
		Parameter:   ft.Parameter,
		CreatedAt:   ft.CreatedAt,
		UpdatedAt:   ft.UpdatedAt,
		StartedAt:   ft.StartedAt,
	}

	err = task.AddTask()

	if err != nil {
		return task, err
	}

	stmt, err := createSqlStmt(`DELETE FROM failed_tasks WHERE id = ?`)
	if err != nil {
		return task, err
	}

	_, err = stmt.Exec(ft.Id)

	return task, err
}

func GetAllFailedTasks() (failedTasks []FailedTask, err error) {
	stmt, err := createSqlStmt(`SELECT * FROM failed_tasks ORDER BY id DESC`)
	if err != nil {
		return failedTasks, err
	}

	rows, err := stmt.Query()
	if err != nil {
		return failedTasks, err
	}

	failedTasks = []FailedTask{}

	defer rows.Close()
	for rows.Next() {
		failedTask := FailedTask{}
		err = rows.Scan(
			&failedTask.Id,
			&failedTask.VideoFormat,
			&failedTask.AudioFormat,
			&failedTask.Url,
			&failedTask.Title,
			&failedTask.OutputPath,
			&failedTask.Parameter,
			&failedTask.CreatedAt,
			&failedTask.UpdatedAt,
			&failedTask.StartedAt,
			&failedTask.FailedAt,
		)
		if err != nil {
			return []FailedTask{}, err
		}
		failedTasks = append(failedTasks, failedTask)
	}

	return failedTasks, err
}
