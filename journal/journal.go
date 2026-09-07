package journal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	ErrUnableToCreateDirectory = errors.New("unable to create dir")
	ErrUnableToCreateFile      = errors.New("unable to create file")
)

type Journal struct {
	Timestamp int64
	dir       string
}

type Entry struct {
	WorkerID int
	JobID    string
	Status   string
	Message  string
}

func New(dir string) (*Journal, error) {
	j := &Journal{
		Timestamp: time.Now().Unix(),
		dir:       dir,
	}

	if err := j.createFile(); err != nil {
		return nil, err
	}

	return j, nil
}

func (j *Journal) createFile() error {
	if err := os.MkdirAll(j.dir, 0755); err != nil {
		return fmt.Errorf("%w:%w", ErrUnableToCreateDirectory, err)
	}

	file, err := os.Create(filepath.Join(j.dir, j.filename()))
	if err != nil {
		return fmt.Errorf("%w:%w", ErrUnableToCreateFile, err)
	}
	defer file.Close()

	return nil
}

func (j *Journal) Append(e Entry) error {
	file, err := os.OpenFile(filepath.Join(j.dir, j.filename()), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	line := fmt.Sprintf("%d,%s,%s,%s\n", e.WorkerID, e.JobID, e.Status, e.Message)
	if _, err := file.WriteString(line); err != nil {
		return err
	}

	return nil
}

func (j *Journal) filename() string {
	return strconv.FormatInt(j.Timestamp, 10) + "_journal.txt"
}
