package Engine

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

func processFiles(path string, pattern string, resultChannel chan<- FileStates) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)
	var lineNumber int32 = 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if len(line) > 0 {

			if strings.Contains(line, pattern) {
				log.Println("Its coming here", lineNumber, path)
				resultChannel <- FileStates{LineNumber: lineNumber, File: path, Content: line}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("error during scan: %s", err)
	}
}

func worker(jobQueue <-chan string, resultChannel chan<- FileStates, pattern string, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobQueue {
		select {
		case <-ctx.Done():
			return
		default:
			processFiles(job, pattern, resultChannel)
		}
	}
}

func InitiateScrapper(path string, pattern string, ctx context.Context, resultChannel chan<- FileStates) {
	wg := sync.WaitGroup{}

	//Queue maintained to add jobs
	jobQueue := make(chan string, 10)

	//Create as many worker routines as cpu cores that can run parallely
	for cpu := 0; cpu < runtime.NumCPU(); cpu++ {
		wg.Add(1)
		go worker(jobQueue, resultChannel, pattern, ctx, &wg)
	}

	go func() {
		targetDir := "test/logs"
		err := filepath.WalkDir(targetDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				fmt.Println("Unable to read this directory", path)
				return err
			}
			if !d.IsDir() {
				jobQueue <- path
				return nil
			}
			return nil
		})
		close(jobQueue)
		if err != nil {
			fmt.Println("Something went wrong", err)
		}
	}()

	wg.Wait()

}
