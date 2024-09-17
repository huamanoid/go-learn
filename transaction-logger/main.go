package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
)

type Event struct {
	Sequence  uint64    // A unique record ID
	EventType EventType // The action taken
	Key       string    // The key affected by this transaction
	Value     string    // The value of a PUT the transaction
}

type EventType byte

const (
	_                     = iota // iota == 0; ignore the zero value
	EventDelete EventType = iota // iota == 1
	EventPut                     // iota == 2; implicitly repeat
	EventGet                     // iota == 3;
)

type FileTransactionLogger struct {
	events       chan<- Event // Write-only channel for sending events
	errors       <-chan error // Read-only channel for receiving errors
	lastSequence uint64       // The last used event sequence number
	file         *os.File     // The location of the transaction log
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *FileTransactionLogger) WriteGet(key, value string) {
	l.events <- Event{EventType: EventGet, Key: key, Value: value}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	WriteGet(key, value string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)

	Run()
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16) // Make an events channel
	l.events = events

	errors := make(chan error, 1) // Make an errors channel
	l.errors = errors

	go func() {
		for e := range events { // Retrieve the next Event

			l.lastSequence++ // Increment sequence number

			_, err := fmt.Fprintf( // Write the event to the log
				l.file,
				"%d\t%d\t%s\t\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file) // Create a Scanner for l.file
	outEvent := make(chan Event)        // An unbuffered Event channel
	outError := make(chan error, 1)     // A buffered error channel

	go func() {
		var e Event

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", &err)
				return
			}

			// Sanity check! Are the sequence numbers in increasing order
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			l.lastSequence = e.Sequence

			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()
	return outEvent, outError
}

var logger TransactionLogger

// Define the struct with embedded RWMutex and a map
var myMap = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

func Get(key string) (string, error) {
	myMap.RLock()
	defer myMap.RUnlock()
	// Check if the key is present
	value, exists := myMap.m[key]
	fmt.Println("GET function: Attempting to get key:", key)

	if !exists {
		fmt.Println("GET function: Key not found")
		return "", ErrorNoSuchKey
	}

	fmt.Println("GET function: Found value:", value)
	return value, nil
}

func Put(key string, value string) error {
	myMap.Lock()
	myMap.m[key] = value
	myMap.Unlock()
	return nil
}

func Delete(key string) error {
	myMap.Lock()
	delete(myMap.m, key)
	myMap.Unlock()
	return nil
}

var ErrorNoSuchKey = errors.New("no such key")

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"] // // Retrieve "key" from the request key := vars["key"]

	value, err := Get(key)
	logger.WriteGet(key, string(value))

	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Write([]byte(value))
}

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	Put(key, string(value))

	logger.WritePut(key, string(value))

	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusCreated)

}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"] // Retrieve "key" from the request key := vars["key"]
	Delete(key)
	logger.WriteDelete(key)
	w.WriteHeader((http.StatusResetContent))
}

func initializeTransactionLog() error {

	var err error

	logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors: // Retrieve any errors
		case e, ok = <-events:
			switch e.EventType {

			case EventDelete:
				err = Delete(e.Key) // Got a DELETE event!
			case EventPut:
				err = Put(e.Key, e.Value) // Got a PUT event!
			case EventGet:
				_, err = Get(e.Key) // Got a GET event!
			}
		}
	}

	logger.Run()

	return err

}

func main() {

	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	initializeTransactionLog()

	fmt.Println("Listening . . .")
	log.Fatal(http.ListenAndServe(":8080", r))

}
