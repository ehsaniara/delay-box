# 3 Workers

This project contains the following components:
- Redis
- Kafka
- Zookeeper
- Kafka UI
- 3 Workers


## Quick Start

Clone the project

```shell
git clone git@github.com:ehsaniara/scheduler.git

cd scheduler
```

make sure to have docker-compose and then run the docker compose as:
```shell
docker-compose -f docker-multi-worker/docker-compose.yml up -d
```

it will build the last image and then runs


- To access kafka-ui: http://localhost:8080/kafka-ui-ts/
