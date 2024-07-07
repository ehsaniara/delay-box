package httpserver

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/ehsaniara/scheduler/config"
	"github.com/ehsaniara/scheduler/core"
	_pb "github.com/ehsaniara/scheduler/proto"
	"github.com/ehsaniara/scheduler/storage"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

type server struct {
	httpPort    int
	httpServer  HTTPServer
	taskStorage storage.TaskStorage
	scheduler   core.Scheduler
	config      *config.Config
	quit        chan struct{}
	ready       chan bool
	stop        sync.Once
}

// HTTPServer is an interface that abstracts the http.Server methods needed by our server.
type HTTPServer interface {
	ListenAndServe() error
	Shutdown(ctx context.Context) error
}

// Ensure http.Server implements HTTPServer interface
var _ HTTPServer = (*http.Server)(nil)

// NewServer should be the last service to be run so K8s know application is fully up
func NewServer(ctx context.Context, httpServer HTTPServer, taskStorage storage.TaskStorage, scheduler core.Scheduler, config *config.Config) func() {
	s := &server{
		httpServer:  httpServer,
		taskStorage: taskStorage,
		config:      config,
		scheduler:   scheduler,
		quit:        make(chan struct{}),
		ready:       make(chan bool, 1),
		httpPort:    config.HttpServer.Port,
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
	TaskUuid           string            `json:"taskUuid"`
	ExecutionTimestamp float64           `json:"executionTimestamp"`
	TaskType           string            `json:"taskType"`
	Header             map[string]string `json:"header"`
	Pyload             string            `json:"pyload"`
}

func (s *server) PingHandler(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

func (s *server) GetAllTasksHandler(c *gin.Context) {

	offsetStr := c.Query("offset")
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		c.String(http.StatusBadRequest, "offset is invalid")
		return
	}

	limitStr := c.Query("limit")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		c.String(http.StatusBadRequest, "limit is invalid")
		return
	}

	var tasks []Task
	for _, task := range s.taskStorage.GetAllTasksPagination(c.Request.Context(), int32(offset), int32(limit)) {
		tasks = append(tasks, Task{
			TaskUuid:           task.TaskUuid,
			ExecutionTimestamp: task.ExecutionTimestamp,
			TaskType:           task.TaskType.String(),
			Header:             ConvertProtoHeaderToHeader(task.Header),
			Pyload:             base64.StdEncoding.EncodeToString(task.Pyload),
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

	taskType, err := StringToTaskType(task.TaskType)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	byteArray, err := base64.StdEncoding.DecodeString(task.Pyload)
	if err != nil {
		fmt.Printf("Error decoding base64 string: %v\n", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	pbTask := &_pb.Task{
		TaskUuid:           task.TaskUuid,
		ExecutionTimestamp: task.ExecutionTimestamp,
		TaskType:           taskType,
		Header:             ConvertHeaderToProtoHeader(task.Header),
		Pyload:             byteArray,
	}

	// enable to use kafka or direct write to redis
	// recommended to enable kafka in high write traffic
	if s.config.Kafka.Enabled {
		s.scheduler.PublishNewTask(pbTask)
	} else {
		s.taskStorage.SetNewTask(c.Request.Context(), pbTask)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "task created",
	})
}

// StringToTaskType converts a taskStr to a Task_Type enum.
func StringToTaskType(taskStr string) (_pb.Task_Type, error) {
	// Look up the enum value by name
	if enumVal, ok := _pb.Task_Type_value[taskStr]; ok {
		return _pb.Task_Type(enumVal), nil
	}
	return _pb.Task_PUB_SUB, fmt.Errorf("invalid enum value: %s", taskStr)
}

// ConvertHeaderToProtoHeader return map[string][]byte
func ConvertHeaderToProtoHeader(header map[string]string) map[string][]byte {
	var pbHeader = make(map[string][]byte)
	for k, v := range header {
		byteArray, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			log.Printf("Error decoding base64 string: %v\n", err)
			continue
		}
		pbHeader[k] = byteArray
	}
	return pbHeader
}

// ConvertProtoHeaderToHeader return map[string]string
func ConvertProtoHeaderToHeader(header map[string][]byte) map[string]string {
	var pbHeader = make(map[string]string)
	for k, v := range header {
		// Encode the serialized protobuf message to a Base64 encoded string
		pbHeader[k] = base64.StdEncoding.EncodeToString(v)
	}
	return pbHeader
}
