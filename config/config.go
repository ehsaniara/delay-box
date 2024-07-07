package config

import (
	"github.com/spf13/viper"
	"log"
	"os"
)

type Config struct {
	HttpServer HttpServerConfig
	Storage    StorageConfig
	Kafka      KafkaConfig
}

type HttpServerConfig struct {
	Port        int
	Mode        string
	ContextPath string
}

type StorageConfig struct {
	RedisPass        string
	RedisHost        string
	RedisDb          int
	Chanel           string
	SchedulerKeyName string
}

type KafkaConfig struct {
	//if disabled then uses redis pub-sub
	Enabled bool
	//broker1:9092,broker2:9092,broker3:9092
	Brokers            string
	Group              string
	SchedulerTopic     string
	TaskExecutionTopic string
}

func GetConfig() *Config {
	path := os.Getenv("APP_CONF_PATH")
	if path == "" {
		path = "./config/config.yaml"
	}

	c, err := LoadConfig(path)
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func Print(config *Config) {
	log.Printf("Server port: %d\n", config.HttpServer.Port)
	log.Printf("Server mode: %s\n", config.HttpServer.Mode)
	log.Printf("Server context path: %s\n", config.HttpServer.ContextPath)
	log.Printf("redisHost: %s\n", config.Storage.RedisHost)
	p := ""
	if len(config.Storage.RedisPass) > 0 {
		p = "***"
	}
	log.Printf("redisPass: %s\n", p)
	log.Printf("redisDb: %d\n", config.Storage.RedisDb)
	log.Printf("chanel: %s\n", config.Storage.Chanel)
	log.Printf("schedulerKeyName: %s\n", config.Storage.SchedulerKeyName)

	log.Printf("kafka enabled: %v\n", config.Kafka.Enabled)
	log.Printf("kafka brokers: %s\n", config.Kafka.Brokers)
	log.Printf("schedulerTopic: %s\n", config.Kafka.SchedulerTopic)
	log.Printf("taskExecutionTopic: %s\n", config.Kafka.TaskExecutionTopic)
}

func LoadConfig(path string) (*Config, error) {
	viper.SetConfigName(path)
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AutomaticEnv() // Automatically override with environment variables

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}
	//to override config
	viper.SetEnvPrefix("app")
	_ = viper.BindEnv("httpServer.contextPath", "SERVER_CONTEXT_PATH")
	_ = viper.BindEnv("storage.redisHost", "REDIS_HOST")
	_ = viper.BindEnv("storage.redisPass", "REDIS_PASS")
	_ = viper.BindEnv("kafka.brokers", "BROKERS")

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}
	return &config, nil
}
