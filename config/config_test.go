package config

//
//import (
//	"github.com/spf13/viper"
//	"github.com/stretchr/testify/assert"
//	"os"
//	"testing"
//)
//
//func TestLoadConfig(t *testing.T) {
//	// Set the environment variable for the test
//	err := os.Setenv("APP_ENV", "test")
//	assert.NoError(t, err)
//
//	// Create a temporary config file for testing
//	configContent := `
//httpServer:
//  port: 8088
//  mode: test
//
//storage:
//  redisHost: localhost:6379
//  redisPass:
//  redisDb: 0
//  chanel: schedule_chanel
//  schedulerKeyName: scheduled_tasks
//
//kafka:
//  enabled: true
//  brokers: localhost:9092
//  group: scheduler
//  schedulerTopic: Scheduler
//  taskExecutionTopic: TaskExecution
//`
//	configFile, err := os.CreateTemp("config", "config.test.yaml")
//	assert.NotNil(t, configFile)
//	assert.NoError(t, err)
//	defer func(name string) {
//		e := os.Remove(name)
//		assert.NoError(t, e)
//	}(configFile.Name())
//
//	_, err = configFile.Write([]byte(configContent))
//	assert.NoError(t, err)
//
//	// Set Viper to read the temporary config file
//	viper.SetConfigFile(configFile.Name())
//
//	config, err := GetConfig()
//	assert.NoError(t, err)
//
//	assert.Equal(t, 8080, config.HttpServer.Port)
//	assert.Equal(t, "test", config.HttpServer.Mode)
//	assert.Equal(t, "scheduled_tasks", config.Storage.SchedulerKeyName)
//	assert.Equal(t, "schedule_chanel", config.Storage.Chanel)
//	assert.Equal(t, 0, config.Storage.RedisDb)
//}
//
//func TestLoadConfigWithEnvOverride(t *testing.T) {
//	// Set the environment variables for the test
//	os.Setenv("APP_ENV", "test")
//	os.Setenv("APP_DATABASE_USER", "envuser")
//	os.Setenv("APP_DATABASE_PASSWORD", "envpassword")
//	os.Setenv("APP_DATABASE_HOST", "envhost")
//	os.Setenv("APP_DATABASE_NAME", "envdatabase")
//
//	// Create a temporary config file for testing
//	configContent := `
//httpServer:
//  port: 8088
//  mode: test
//
//storage:
//  redisHost: localhost:6379
//  redisPass:
//  redisDb: 0
//  chanel: schedule_chanel
//  schedulerKeyName: scheduled_tasks
//
//kafka:
//  enabled: true
//  brokers: localhost:9092
//  group: scheduler
//  schedulerTopic: Scheduler
//  taskExecutionTopic: TaskExecution
//`
//	configFile, err := os.CreateTemp("", "config.test.yaml")
//	assert.NoError(t, err)
//	defer os.Remove(configFile.Name())
//
//	_, err = configFile.Write([]byte(configContent))
//	assert.NoError(t, err)
//
//	// Set Viper to read the temporary config file
//	viper.SetConfigFile(configFile.Name())
//
//	config, err := GetConfig()
//	assert.NoError(t, err)
//
//	assert.Equal(t, 8080, config.HttpServer.Port)
//	assert.Equal(t, "test", config.HttpServer.Mode)
//	assert.Equal(t, "scheduled_tasks", config.Storage.SchedulerKeyName)
//	assert.Equal(t, "schedule_chanel", config.Storage.Chanel)
//	assert.Equal(t, 0, config.Storage.RedisDb)
//}
