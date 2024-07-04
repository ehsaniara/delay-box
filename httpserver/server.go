package httpserver

import (
	"context"
	"errors"
	"fmt"
	"github.com/ehsaniara/scheduler/storage"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"sync"
	"time"
)

// You only need **one** of these per package!
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

type server struct {
	httpPort    int
	httpServer  HTTPServer
	taskStorage storage.TaskStorage
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
func NewServer(ctx context.Context, httpPort int, httpServer HTTPServer, taskStorage storage.TaskStorage) func() {
	s := &server{
		httpServer:  httpServer,
		taskStorage: taskStorage,
		quit:        make(chan struct{}),
		ready:       make(chan bool, 1),
		httpPort:    httpPort,
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
	r.GET("/ping", s.PingHandler)
	r.GET("/task", s.GetAllTasksHandler)
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
	ExecutionTimestamp float64           `json:"executionTimestamp"`
	Header             map[string][]byte `json:"header"`
	Pyload             []byte            `json:"pyload"`
}

func (s *server) PingHandler(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

func (s *server) GetAllTasksHandler(c *gin.Context) {
	s.taskStorage.GetAllTasks(c.Request.Context(), 0, 0)
	c.String(http.StatusOK, "pong")
}
