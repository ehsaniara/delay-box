package e2e

//func TestPositiveIntegrationTest(t *testing.T) {
//
//	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
//	defer cancel()
//
//	redisContainer, kafkaContainer, err := setupContainers(ctx)
//	assert.NoError(t, err)
//
//	brokers, err := kafkaContainer.Brokers(ctx)
//
//	// Get the Redis container's address
//	redisHost, err := redisContainer.Host(ctx)
//	assert.NoError(t, err)
//	redisPort, err := redisContainer.MappedPort(ctx, "6379")
//	assert.NoError(t, err)
//	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())
//
//	err = os.Setenv("REDIS_HOST", redisAddr)
//	assert.NoError(t, err)
//	err = os.Setenv("BROKERS", brokers[0])
//	assert.NoError(t, err)
//	defer func() {
//		_ = os.Unsetenv("APP_CONF_PATH")
//		_ = os.Unsetenv("REDIS_HOST")
//		_ = os.Unsetenv("BROKERS")
//
//		// Clean up the container
//		if err = redisContainer.Terminate(ctx); err != nil {
//			log.Fatalf("failed to terminate container: %s", err)
//		}
//
//		// Clean up the container after
//		if err = kafkaContainer.Terminate(ctx); err != nil {
//			log.Fatalf("failed to terminate container: %s", err)
//		}
//	}()
//
//	// Start the server
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go runMainProcessInParallel(&wg)
//	wg.Done()
//
//	//
//	err = os.Setenv("APP_CONF_PATH", "./kafka-test.yaml")
//	assert.NoError(t, err)
//
//	c := config.GetConfig()
//
//	var redisClient, redisClientCloseFn = storage.NewRedisClient(ctx, c)
//	defer redisClientCloseFn()
//
//	// Wait for the server to start
//	ready := make(chan error)
//	go func() {
//		for {
//			time.Sleep(time.Second)
//			// Perform a GET request
//			resp, e := http.Get(fmt.Sprintf("http://localhost:%d/ping", c.HttpServer.Port))
//			if e != nil {
//				t.Logf("Failed to make GET request: %v", e)
//			} else {
//				if resp.StatusCode == http.StatusOK {
//					err = resp.Body.Close()
//					assert.NoError(t, err)
//					ready <- nil
//				}
//			}
//		}
//	}()
//
//	readyErr := <-ready
//	assert.NoError(t, readyErr)
//
//	//put new task in kafka
//	sc := sarama.NewConfig()
//	// set config to true because successfully delivered messages will be returned on the Successes channel
//	sc.Producer.Return.Successes = true
//	producer, err := sarama.NewSyncProducer(brokers, sc)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	// Schedule 1/4 seconds from now
//	executionTime := float64(time.Now().Add(time.Millisecond * 250).UnixMilli())
//	task := _task.Task{
//		TaskUuid: uuid.NewString(),
//		Header:   map[string][]byte{config.ExecutionTimestamp: []byte(fmt.Sprintf("%v", executionTime))},
//		Pyload:   []byte("some payload data"),
//	}
//	m, e := proto.Marshal(&task)
//	assert.NoError(t, e)
//
//	if _, _, err = producer.SendMessage(&sarama.ProducerMessage{
//		Topic: c.Kafka.SchedulerTopic,
//		Value: sarama.ByteEncoder(m),
//		Headers: []sarama.RecordHeader{{
//			Key:   []byte("executionTimestamp"),
//			Value: []byte(fmt.Sprintf("%v", executionTime)),
//		}},
//	}); err != nil {
//		t.Fatal(err)
//	}
//	//check if it's in redis, to make sure message get dispatched to redis from consumer
//	time.Sleep(10 * time.Millisecond)
//
//	count, err := redisClient.CountAllWaitingTasks(ctx)
//	assert.NoError(t, err)
//
//	// task should be in the redis, it's not expired yet
//	assert.Equal(t, int64(1), count)
//
//	//check that task got erased after that, task has only 1 sec TTL
//	time.Sleep(1005 * time.Millisecond)
//
//	count, err = redisClient.CountAllWaitingTasks(ctx)
//	assert.NoError(t, err)
//
//	assert.Equal(t, int64(0), count)
//
//	//create consumer on task execute topic
//	wg.Add(1)
//	go consumerForTaskExecutionTopic(t, ctx, &wg, c, &task)
//
//	wg.Wait()
//	fmt.Println("All waitGroups done")
//}
