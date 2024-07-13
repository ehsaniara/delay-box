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

Following task is type of `SHELL_CMD` which mean it will execute at any defined worker nodes on the given timestamp, (Payload is just the OS Date command)
```shell
curl -X POST http://localhost:8088/api/task  \
  -H "Content-Type: application/json" -d \
  '{"parameter":{"executionTimestamp":"1720672590913","taskType":"SHELL_CMD"},"pyload":"ZGF0ZQ=="}'
```

To Get list of pending tasks (First 100 tasks)
```shell
curl "http://localhost:8088/task"
```
## Clean up

```shell
docker-compose -f docker-multi-worker/docker-compose.yml down -v
```
