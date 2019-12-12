package queue

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"time"
)

type Task struct {
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
}

func (t Task) String() string {
	return fmt.Sprintf(
		"Id: %s\tVideoFormat:%s\tAudioFormat:%s\tUrl:%s\tTitle:%s\tOutputPath:%s\tParameter:%s",
		t.Id,
		t.VideoFormat,
		t.AudioFormat,
		t.Url,
		t.Title,
		t.OutputPath,
		t.Parameter,
	)
}

func (t *Task) Exec(limits chan struct{}, wg sync.WaitGroup) (err error) {
	defer func() {
		<-limits
		wg.Done()
	}()

	limits <- struct{}{}

	params := []string{
		"--ffmpeg-location", ffmpegPath, // ffmpeg path
		"-f", t.VideoFormat + "+" + t.AudioFormat, // format
		"-o", t.OutputPath, // file output
	}

	if t.Parameter != "" {
		params = append(params, t.Parameter)
	}

	params = append(params, t.Url)

	// youtube-dl execute log path
	sep := string(os.PathSeparator)
	taskLogFile, err := os.OpenFile(
		logDirectory+sep+t.Id+".log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0666,
	)
	if err != nil {
		return err
	}

	defer taskLogFile.Close()

	return t.Command(taskLogFile, youtubeDlPath, params...)
}

func (t *Task) Command(file *os.File, path string, params ...string) error {
	command := exec.Command(path, params...)
	command.Stdout = file
	command.Stderr = file
	err := command.Start()
	if err != nil {
		return err
	}

	return command.Wait()
}

func (t *Task) SetId() error {
	if t.Id != "" {
		return nil
	}

	urlStruct, err := url.Parse(t.Url)

	if err != nil {
		return err
	}

	query := urlStruct.Query()
	if v, ok := query["v"]; ok {
		t.Id = v[0]
		return nil
	} else {
		return errors.New("cannot find id.")
	}
}

func (t *Task) AddTask() (err error) {
	stmt, err := createSqlStmt(`INSERT INTO tasks (id, video_format, audio_format, url, title, output_path, parameter, created_at, updated_at, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	if err := t.SetId(); err != nil {
		return err
	}

	_, err = stmt.Exec(
		t.Id,
		t.VideoFormat,
		t.AudioFormat,
		t.Url,
		t.Title,
		t.OutputPath,
		t.Parameter,
		t.CreatedAt,
		t.UpdatedAt,
		t.StartedAt,
	)

	return err
}

func (t *Task) QueueTask() (err error) {
	t.CreatedAt = time.Now().Unix()
	t.UpdatedAt = time.Now().Unix()

	return t.AddTask()
}

func (t *Task) StartTask() (err error) {
	stmt, err := createSqlStmt(`UPDATE tasks SET started_at = ? WHERE id = ?`)
	if err != nil {
		return err
	}

	t.StartedAt = time.Now().Unix()
	_, err = stmt.Exec(t.StartedAt, t.Id)

	return err
}

// Task削除
func (t *Task) FinishTask() (err error) {
	stmt, err := createSqlStmt(`DELETE FROM tasks WHERE id = ?`)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(t.Id)

	return err
}

func (t *Task) AddFailedTask() (failedTask FailedTask, err error) {
	failedTask = FailedTask{
		Id:          t.Id,
		VideoFormat: t.VideoFormat,
		AudioFormat: t.AudioFormat,
		Url:         t.Url,
		Title:       t.Title,
		OutputPath:  t.OutputPath,
		Parameter:   t.Parameter,
		CreatedAt:   t.CreatedAt,
		UpdatedAt:   t.UpdatedAt,
		StartedAt:   t.StartedAt,
	}

	err = failedTask.AddTask()

	return failedTask, err
}

func popTasks() (tasks []Task, err error) {
	stmt, err := createSqlStmt(`SELECT * FROM tasks ORDER BY created_at ASC LIMIT ?`)
	if err != nil {
		return tasks, err
	}

	rows, err := stmt.Query(workerNum)
	if err != nil {
		return tasks, err
	}

	tasks = make([]Task, 0, workerNum)

	defer rows.Close()
	for rows.Next() {
		task := Task{}
		err = rows.Scan(
			&task.Id,
			&task.VideoFormat,
			&task.AudioFormat,
			&task.Url,
			&task.Title,
			&task.OutputPath,
			&task.Parameter,
			&task.CreatedAt,
			&task.UpdatedAt,
			&task.StartedAt,
		)
		if err != nil {
			return []Task{}, err
		}
		tasks = append(tasks, task)
	}

	return tasks, err
}

func GetAllTasks() (tasks []Task, err error) {
	stmt, err := createSqlStmt(`SELECT * FROM tasks ORDER BY created_at DESC, updated_at DESC`)
	if err != nil {
		return tasks, err
	}

	rows, err := stmt.Query()
	if err != nil {
		return tasks, err
	}

	tasks = []Task{}

	defer rows.Close()
	for rows.Next() {
		task := Task{}
		err = rows.Scan(
			&task.Id,
			&task.VideoFormat,
			&task.AudioFormat,
			&task.Url,
			&task.Title,
			&task.OutputPath,
			&task.Parameter,
			&task.CreatedAt,
			&task.UpdatedAt,
			&task.StartedAt,
		)
		if err != nil {
			return []Task{}, err
		}
		tasks = append(tasks, task)
	}

	return tasks, err
}

// 後でファイル移動
func GetTasksMapByIds(ids []string) (tasks []Task, err error) {
	stmt, err := createSqlStmt(`SELECT * FROM tasks WHERE id IN (?) ORDER BY created_at DESC, updated_at DESC`)
	if err != nil {
		return tasks, err
	}

	rows, err := stmt.Query(ids)
	if err != nil {
		return tasks, err
	}

	tasks = []Task{}

	defer rows.Close()
	for rows.Next() {
		task := Task{}
		err = rows.Scan(
			&task.Id,
			&task.VideoFormat,
			&task.AudioFormat,
			&task.Url,
			&task.Title,
			&task.OutputPath,
			&task.Parameter,
			&task.CreatedAt,
			&task.UpdatedAt,
			&task.StartedAt,
		)
		if err != nil {
			return []Task{}, err
		}

		tasks = append(tasks, task)
	}

	return tasks, err
}
