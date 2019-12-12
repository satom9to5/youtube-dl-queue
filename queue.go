package queue

import (
	"database/sql"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/satom9to5/pidfile"
)

var (
	youtubeDlPath string
	ffmpegPath    string
	logDirectory  = "./log"
	starting      = false
	workerNum     = 1
	dispatch      = runWorker
)

func Start(varDB *sql.DB, pidfilePath string, varYoutubeDlPath string, varFFmpegPath string) (pid int, err error) {
	if starting {
		return pid, errors.New("worker is already start.")
	}

	pidfile.Initialize(pidfilePath)
	if err = writePidfile(); err != nil {
		return pid, err
	}

	if err = InitializeSchema(varDB); err != nil {
		return pid, err
	}

	if varYoutubeDlPath == "" {
		return pid, errors.New("youtube-dl path is empty.")
	}

	youtubeDlPath = varYoutubeDlPath

	starting = true

	go dispatch()

	return pidfile.Read()
}

func Stop() {
	starting = false
	CloseDB()
	pidfile.Remove()
}

func SetLogDirectory(varLogDirectory string) {
	logDirectory = varLogDirectory
}

func writePidfile() error {
	pid, _ := pidfile.Read()
	if pid > 0 {
		return errors.New("Server is already Running.")
	}

	return pidfile.Write()
}

func runWorker() (err error) {
	limits := make(chan struct{}, workerNum)

	var wg sync.WaitGroup

	for {
		tasks, err := popTasks()
		if err != nil {
			log.Println(err)
			continue
		}
		if len(tasks) == 0 {
			time.Sleep(time.Minute)
			continue
		}

		// ひとまず複数実行はしないが後で治す
		for _, task := range tasks {
			wg.Add(1)
			go task.Exec(limits, wg)
		}

		wg.Wait()
	}

	return nil
}
