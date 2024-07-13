package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ehsaniara/delay-box/config"
	"github.com/ehsaniara/delay-box/httpserver"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"
)

func TestHttpBasedIntegrationTest(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	redisContainer, kafkaContainer, err := setupContainers(ctx)
	assert.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)

	// Get the Redis container's address
	redisHost, err := redisContainer.Host(ctx)
	assert.NoError(t, err)
	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	assert.NoError(t, err)
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	err = os.Setenv("REDIS_HOST", redisAddr)
	assert.NoError(t, err)
	err = os.Setenv("BROKERS", brokers[0])
	assert.NoError(t, err)
	defer func() {
		_ = os.Unsetenv("APP_CONF_PATH")
		_ = os.Unsetenv("REDIS_HOST")
		_ = os.Unsetenv("BROKERS")

		// Clean up the container
		if err = redisContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}

		// Clean up the container after
		if err = kafkaContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}
	}()

	// Start the server
	var wg sync.WaitGroup
	wg.Add(1)
	go runMainProcessInParallel(&wg)
	wg.Done()

	//
	err = os.Setenv("APP_CONF_PATH", "./kafka-test.yaml")
	assert.NoError(t, err)

	c := config.GetConfig()

	// Wait for the server to start
	ready := make(chan error)
	go func() {
		for {
			time.Sleep(time.Second)
			// Perform a GET request
			resp, e := http.Get(fmt.Sprintf("http://localhost:%d/ping", c.HttpServer.Port))
			if e != nil {
				t.Logf("Failed to make GET request: %v", e)
			} else {
				if resp.StatusCode == http.StatusOK {
					err = resp.Body.Close()
					assert.NoError(t, err)
					ready <- nil
				}
			}
		}
	}()

	readyErr := <-ready
	assert.NoError(t, readyErr)

	// check there is not task waiting

	res1, err := http.Get(fmt.Sprintf("http://localhost:%d/task", c.HttpServer.Port))
	assert.NoError(t, err)

	assert.Equal(t, http.StatusOK, res1.StatusCode)

	bodyBytes, err := io.ReadAll(res1.Body)
	assert.NoError(t, err)
	bodyString := string(bodyBytes)
	_ = res1.Body.Close()

	assert.Equal(t, "[]", bodyString)

	executionTimestamp := time.Now().Add(time.Second).UnixMilli()

	totalTasks := 10
	// add short 10 tasks
	wg.Add(totalTasks)
	for i := 0; i < totalTasks; i++ {
		go func() {
			defer wg.Done()
			payload := fmt.Sprintf(`{"parameter":{"executionTimestamp":"%d", "taskType":"PUB_SUB"},"pyload":"VGVzdCBKYXkK"}`, executionTimestamp)
			url := fmt.Sprintf("http://localhost:%d/task", c.HttpServer.Port)

			// post the task
			res2, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(payload)))
			assert.NoError(t, err)

			assert.Equal(t, http.StatusOK, res2.StatusCode)
			_ = res2.Body.Close()
		}()
	}
	// gress period to publish the task
	time.Sleep(500 * time.Millisecond)
	// then check they are there
	res2, err := http.Get(fmt.Sprintf("http://localhost:%d/task", c.HttpServer.Port))
	assert.NoError(t, err)

	var task []httpserver.Task
	err = json.NewDecoder(res2.Body).Decode(&task)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, totalTasks, len(task))
	_ = res2.Body.Close()

	// wait to be executed
	time.Sleep(1500 * time.Millisecond)

	res3, err := http.Get(fmt.Sprintf("http://localhost:%d/task", c.HttpServer.Port))
	assert.NoError(t, err)
	bodyBytes2, err := io.ReadAll(res3.Body)

	bodyString2 := string(bodyBytes2)
	_ = res3.Body.Close()
	assert.Equal(t, "[]", bodyString2)

	// done
	wg.Wait()
	fmt.Println("All waitGroups done")
}
