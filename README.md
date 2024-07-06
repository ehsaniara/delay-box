# Task Scheduler
Distributed task scheduler

[![Go](https://github.com/ehsaniara/scheduler/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/ehsaniara/scheduler/actions/workflows/go.yml)

Set Task
```shell
curl -X POST http://localhost:8088/task -H "Content-Type: application/json" -d '{"executionTimestamp":1720285291097,"pyload":"VGVzdCBKYXkK", "taskType":"PUB_SUB"}'
```


Get list of pending tasks
```shell
curl "http://localhost:8088/task?offset=100&limit=0"
```
