package httpserver

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/ehsaniara/delay-box/config"
	"github.com/ehsaniara/delay-box/core/corefakes"
	scheduler "github.com/ehsaniara/delay-box/proto"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func Test_convertHeaderToParameter(t *testing.T) {
	tests := []struct {
		name string
		args map[string][]byte
		want map[string]string
	}{
		{name: "positive test", args: map[string][]byte{"test": []byte("test")}, want: map[string]string{"test": "test"}},
		{name: "empty arg", args: make(map[string][]byte), want: make(map[string]string)},
		{name: "nil arg", args: nil, want: make(map[string]string)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertHeaderToParameter(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertHeaderToParameter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPingHandler(t *testing.T) {
	// Create a new Gin engine and context
	gin.SetMode(gin.TestMode)

	router := gin.New()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	// Create a mock scheduler
	s := &server{}

	// Register the handler function
	router.GET("/task", s.PingHandler)

	// Create a new HTTP request
	req, err := http.NewRequest("GET", "/ping", nil)
	req.Header.Set(ContentType, ContentTypeJSON)
	assert.NoError(t, err)

	// Set the request context to the Gin context
	ctx.Request = req

	// Call the handler function directly
	s.PingHandler(ctx)

	// Verify the response
	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, `pong`, recorder.Body.String())
}

func TestSetNewTaskHandler(t *testing.T) {
	// Create a new Gin engine and context
	gin.SetMode(gin.TestMode)

	router := gin.New()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	fakeScheduler := &corefakes.FakeScheduler{}

	// Create a mock scheduler
	s := &server{
		scheduler: fakeScheduler,
	}

	encoded := base64.StdEncoding.EncodeToString([]byte("samplePayload"))
	// Create a new task JSON
	task := &Task{
		Pyload: encoded,
		Parameter: map[string]string{
			"test": "dGVzdA==",
		},
	}
	taskJSON, err := json.Marshal(task)
	assert.NoError(t, err)
	// Register the handler function
	router.GET("/task", s.GetAllTasksHandler)

	// Create a new HTTP request
	req, err := http.NewRequest("POST", "/task", bytes.NewBuffer(taskJSON))
	req.Header.Set(ContentType, ContentTypeJSON)
	assert.NoError(t, err)

	// Set the request context to the Gin context
	ctx.Request = req

	// Call the handler function directly
	s.SetNewTaskHandler(ctx)

	// Verify the response
	assert.Equal(t, http.StatusOK, recorder.Code)
	expectedResponse := `{"message":"task created"}`
	assert.JSONEq(t, expectedResponse, recorder.Body.String())
}

func TestGetAllTasksHandler(t *testing.T) {
	// Create a new Gin engine and context
	gin.SetMode(gin.TestMode)
	router := gin.New()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	fakeScheduler := &corefakes.FakeScheduler{}
	// Create a mock scheduler
	s := &server{
		scheduler:                fakeScheduler,
		convertHeaderToParameter: convertHeaderToParameter,
	}

	// Register the handler function
	router.GET("/task", s.GetAllTasksHandler)

	// Create a new HTTP request with query parameters
	// not passing offset and limits
	req, err := http.NewRequest("GET", "/task", nil)
	assert.NoError(t, err)

	// Set the request context to the Gin context
	ctx.Request = req

	uuid1 := "0eeb435b-3099-4750-9fb4-a5968032269b"
	uuid2 := "7877f20e-c544-432e-bd57-4a5c883e3f30"

	fakeScheduler.GetAllTasksPaginationReturns([]*scheduler.Task{
		{
			TaskUuid: uuid1,
			Pyload:   []byte("samplePayload"),
			Header: map[string][]byte{
				config.ExecutionTimestamp: []byte("1111111111"),
			},
		},
		{
			TaskUuid: uuid2,
			Pyload:   []byte("samplePayload"),
			Header: map[string][]byte{
				config.ExecutionTimestamp: []byte("1111111111"),
			},
		},
	})

	// Call the handler function directly
	s.GetAllTasksHandler(ctx)

	// Verify the response
	assert.Equal(t, http.StatusOK, recorder.Code)
	expectedResponse := `[{"taskUuid":"0eeb435b-3099-4750-9fb4-a5968032269b","parameter":{"executionTimestamp":"1111111111"},"pyload":"c2FtcGxlUGF5bG9hZA==","status":""},{"taskUuid":"7877f20e-c544-432e-bd57-4a5c883e3f30","parameter":{"executionTimestamp":"1111111111"},"pyload":"c2FtcGxlUGF5bG9hZA==","status":""}]`
	assert.JSONEq(t, expectedResponse, recorder.Body.String())
}
