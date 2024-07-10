package httpserver

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/core"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type server struct {
	httpPort                 int
	httpServer               HTTPServer
	scheduler                core.Scheduler
	config                   *config.Config
	quit                     chan struct{}
	ready                    chan bool
	stop                     sync.Once
	convertHeaderToParameter ConvertHeaderToParameterFn
}

// HTTPServer is an interface that abstracts the http.Server methods needed by our server.
type HTTPServer interface {
	ListenAndServe() error
	Shutdown(ctx context.Context) error
}

// Ensure http.Server implements HTTPServer interface
var _ HTTPServer = (*http.Server)(nil)

// NewServer should be the last service to be run so K8s know application is fully up
func NewServer(ctx context.Context, httpServer HTTPServer, scheduler core.Scheduler, config *config.Config) func() {
	s := &server{
		httpServer:               httpServer,
		config:                   config,
		scheduler:                scheduler,
		quit:                     make(chan struct{}),
		ready:                    make(chan bool, 1),
		httpPort:                 config.HttpServer.Port,
		convertHeaderToParameter: convertHeaderToParameter,
	}
	go s.runServer(ctx)

	<-s.ready
	log.Printf("✔️ Server is running on port %v\n", s.httpPort)
	return s.stopServer
}

func (s *server) stopServer() {
	s.stop.Do(func() {
		log.Println("👍 Server stopping...")
		close(s.quit)
	})
}

func (s *server) setupGinRouter() *gin.Engine {

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.GET(fmt.Sprintf("%sping", s.config.HttpServer.ContextPath), s.PingHandler)
	r.GET(fmt.Sprintf("%stask", s.config.HttpServer.ContextPath), s.GetAllTasksHandler)
	r.POST(fmt.Sprintf("%stask", s.config.HttpServer.ContextPath), s.SetNewTaskHandler)
	return r
}

func (s *server) runServer(ctx context.Context) {
	if s.httpPort == 0 {
		log.Fatal("HTTP_PORT environment variable is missing")
	}

	// at test case it will be mocked
	if s.httpServer == nil {
		s.httpServer = &http.Server{
			Addr:              fmt.Sprintf(":%v", s.httpPort),
			ReadHeaderTimeout: 3 * time.Second,
			Handler:           s.setupGinRouter(),
		}
	}

	serverErrors := make(chan error, 1)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			select {
			case serverErrors <- err:
			default:
			}
		}
	}()

	s.ready <- true

	select {
	case <-s.quit:
		log.Println("Shutting down the server...")

		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := s.httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Error during server shutdown: %v", err)
		}

		log.Println("Server stopped gracefully")

	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server encountered an error: %v", err)
		}
	}

	close(serverErrors)
}

type Task struct {
	TaskUuid  string            `json:"taskUuid"`
	TaskType  string            `json:"taskType"`
	Parameter map[string]string `json:"parameter,omitempty"`
	Pyload    string            `json:"pyload"`
	Status    string            `json:"status"`
}

func (s *server) PingHandler(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

func (s *server) GetAllTasksHandler(c *gin.Context) {

	offsetStr := c.Query("offset")
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		//c.String(http.StatusBadRequest, "invalid url offset")
		log.Println("invalid url offset, setting it to default value: 100")
		offset = 100
	}

	limitStr := c.Query("limit")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		//c.String(http.StatusBadRequest, "invalid url limit")
		log.Println("invalid url limit, setting it to default value: 0")
		limit = 0
	}

	var tasks []Task
	for _, task := range s.scheduler.GetAllTasksPagination(c.Request.Context(), int32(offset), int32(limit)) {
		tasks = append(tasks, Task{
			TaskUuid:  task.TaskUuid,
			TaskType:  task.TaskType.String(),
			Parameter: s.convertHeaderToParameter(task.Header),
			Pyload:    base64.StdEncoding.EncodeToString(task.Pyload),
		})
	}
	if len(tasks) > 0 {
		c.JSON(http.StatusOK, tasks)
	} else {
		c.JSON(http.StatusOK, make([]string, 0))
	}
}

func (s *server) SetNewTaskHandler(c *gin.Context) {
	var task *Task

	// Bind the JSON to the newItem struct
	if err := c.ShouldBindJSON(&task); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := s.scheduler.Schedule(task.TaskType, task.Pyload, task.Parameter)
	if err != nil {
		fmt.Printf("Error decoding base64 string: %v\n", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "task created",
	})
}

// ConvertHeaderToParameterFn use as injection
type ConvertHeaderToParameterFn func(header map[string][]byte) map[string]string

// ConvertHeaderToParameter return map[string]string
func convertHeaderToParameter(header map[string][]byte) map[string]string {
	var pbHeader = make(map[string]string)
	for k, v := range header {
		// Encode the serialized protobuf message to a Base64 encoded string
		pbHeader[k] = base64.StdEncoding.EncodeToString(v)
	}
	return pbHeader
}
