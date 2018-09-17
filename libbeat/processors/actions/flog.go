package actions

import (
	"os"
	"os/signal"
	"syscall"
	"fmt"
)

// Logger represents some form of message recipient that is responsible
// for absorbing log messages and doing something interesting with the
// messages.
type Logger interface {
	log(string)
}

// Flog is an instance of a file logger which is responsible for
// closing the log file in response to sigint and sigterm.
type Flog struct {
	filename string
	file *os.File
	closed bool
	messages chan string
	closeSig chan os.Signal
}

// NewFlog creates a Logger to write against the given file.  If
// the file doesn't exist or it can't write to that file due to permissions
// or for any other errors, the function will return nil and an error.
//
// The Logger will close the file in response to an sigint or a sigterm.
func NewFlog(filename string) (*Flog, error) {
	file, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	logger := &Flog{
		filename: filename,
		file: file,
		messages: make(chan string, 10),
		closeSig: make(chan os.Signal, 1),
	}

	go logger.run()

	return logger, nil
}

// run simply listens for messages and os signals and does
// the right action.
func (f *Flog) run() {
	running := true
	signal.Notify(f.closeSig, syscall.SIGINT, syscall.SIGTERM)

	defer func() {
		f.file.Close()
		fmt.Println("closing file")
	}()

	for running {
		select {
		case <-f.closeSig:
			fmt.Println("closing the Logger")
			running = false

		case msg := <- f.messages:
			_, err := f.file.Write([]byte(msg))
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}

	fmt.Println("Logger closed")
}

// log accepts log lines and forwards them to the internal queue to
// be written to file.
func (f *Flog) log(msg string) {
	f.messages <- msg
}

// logf is much like printf, but sent to log.
func (f *Flog) logf(format string, a ...interface{}) {
	f.messages <- fmt.Sprintln(fmt.Sprintf(format, a...))
}
