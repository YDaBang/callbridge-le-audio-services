package safelog

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

type Writer struct {
	mu      sync.Mutex
	path    string
	maximum int64
	file    *os.File
	written int64
}

func Open(path string, maximum int64) (*Writer, error) {
	if !filepath.IsAbs(path) || maximum < 64<<10 || maximum > 64<<20 {
		return nil, errors.New("invalid log writer configuration")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	writer := &Writer{path: path, maximum: maximum}
	if err := writer.open(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *Writer) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.written+int64(len(data)) > w.maximum {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	count, err := w.file.Write(data)
	w.written += int64(count)
	return count, err
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *Writer) open() error {
	file, err := os.OpenFile(w.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	w.file = file
	w.written = info.Size()
	return nil
}

func (w *Writer) rotate() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}
	_ = os.Remove(w.path + ".1")
	if err := os.Rename(w.path, w.path+".1"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return w.open()
}
