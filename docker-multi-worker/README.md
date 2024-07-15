
# 3 Workers

This project contains the following components:
- **Redis**: In-memory data structure store used as a database, cache, and message broker.
- **Kafka**: Distributed event streaming platform capable of handling trillions of events a day.
- **Zookeeper**: Centralized service for maintaining configuration information, naming, providing distributed synchronization, and providing group services.
- **Kafka UI**: Web-based interface to manage and monitor Kafka clusters.
- **3 Workers**: Task workers responsible for executing scheduled tasks. Each worker can process tasks independently, providing horizontal scalability.
- **kafka-create-topic**: Ephemeral container to create topics with 3 replicas by calling [create_kafka_topic.sh](create_kafka_topic.sh).

## Quick Start

Make sure to have Docker Compose installed on your machine, then run the Docker Compose command:

```shell
docker-compose up -d
```

This command builds the latest images and starts the services in detached mode.

- To access Kafka UI, navigate to: [http://localhost:8080/kafka-ui-ts/](http://localhost:8080/kafka-ui-ts/)

Wait until all workers are up and running (you'll see a message in their console: "🚀 scheduler is ready!").

Then, run the following command. The task type is `SHELL_CMD`, which means it will execute on any defined worker nodes at the given timestamp. The payload is a simple OS date command.

```shell
sh ./create-task.sh
```

To get a list of pending tasks (first 100 tasks) from your local machine, run:

```shell
curl "http://localhost:8088/task"
```

## Clean up

To stop and remove the containers, volumes, and networks created by Docker Compose, run:

```shell
docker-compose -f docker-multi-worker/docker-compose.yml down -v
```
