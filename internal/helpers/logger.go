package helpers

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// InitServiceLogger writes logs to logs/dev/<service>.log and stdout.
func InitServiceLogger(service string) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	svc := strings.TrimSpace(strings.ToLower(service))
	if svc == "" {
		svc = "app"
	}
	dir := filepath.Join("logs", "dev")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("create log dir failed dir=%s err=%v", dir, err)
		return
	}
	path := filepath.Join(dir, svc+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("open log file failed path=%s err=%v", path, err)
		return
	}
	log.SetOutput(io.MultiWriter(os.Stdout, f))
}
