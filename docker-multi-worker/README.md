# 3 Workers

This project contains the following components:
- Redis
- Kafka
- Zookeeper
- Kafka UI
- 3 Workers


## Quick Start

Make sure to have docker-compose in your machine and then run the docker compose as:
```shell
docker-compose -f docker-multi-worker/docker-compose.yml up -d
```

it will build the last image and then runs


- To access kafka-ui: http://localhost:8080/kafka-ui-ts/

Set Task
```shell
curl -X POST http://localhost:8088/api/task -H "Content-Type: application/json" -d '{"executionTimestamp":1720285291097,"pyload":"VGVzdCBKYXkK", "taskType":"PUB_SUB"}'
```


Get list of pending tasks
```shell
curl "http://localhost:8088/api/task?offset=100&limit=0"
```

## Clean up

```shell
docker-compose -f docker-multi-worker/docker-compose.yml down -v
```
