# DelayBox


[![Go](https://github.com/ehsaniara/delay-box/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/ehsaniara/delay-box/actions/workflows/go.yml)



<p align="center">
  <img src="docs/delay-box-logo.png" alt="scheduler logo"/>
</p>

**DelayBox** is a High Throughput Distributed Task Scheduler ⚡. It is an advanced system designed to manage and execute a vast number of tasks across a distributed network of servers. Built on top of the **Redis** database, it leverages Redis's high-speed in-memory data store 🔥 for quick access and efficient task management.

This scheduler ensures optimal performance and reliability by utilizing protobuf for efficient data serialization and gRPC for robust communication between services. It features intelligent load balancing, dynamic scaling, and fault tolerance to handle high volumes of concurrent tasks without bottlenecks.

🎨 DelayBox also includes a centralized task delay mechanism, allowing precise control over task execution timings, which is critical for workflows requiring synchronized or delayed task processing. Additionally, the system offers the flexibility to integrate with **Kafka**, enabling seamless event streaming and message queuing for enhanced data processing capabilities.

🎨 One of the standout features of this application is its easy horizontal scalability, allowing it to grow and adapt to increasing workloads effortlessly by adding more servers to the network. Equipped with sophisticated algorithms for task prioritization and resource allocation, this scheduler ensures that critical tasks are executed promptly.



# Use-Case

🚀 This tool simplifies the workflow for DevOps engineers, system engineers, and IT experts by removing the need to write code to handle Redis and Kafka development complexities. It manages these tasks for you through straightforward REST calls.

✅ It is ideal for applications requiring massive parallel processing capabilities, such as data processing pipelines, large-scale simulations, and real-time analytics.


# Examples

You can try the docker compose example with 3 worker nodes example [here]([docker-multi-worker](docker-multi-worker)) 

Following task is type of `SHELL_CMD` which mean it will execute at any defined worker nodes on the given timestamp, (Payload is just the OS Date command)
```shell
curl -X POST http://localhost:8088/task  \
  -H "Content-Type: application/json" -d \
  '{"parameter":{"executionTimestamp":"1720672590913","taskType":"SHELL_CMD"},"pyload":"ZGF0ZQ=="}'
```

To Get list of pending tasks (First 100 tasks)
```shell
curl "http://localhost:8088/task"
```

### pyload
This fild is stored as byte format and published in kafka topic `taskExecutionTopic`


### parameter

| parameter name     | type   | required | description                                                                                                  |
|--------------------|--------|----------|--------------------------------------------------------------------------------------------------------------|
| taskType           | string | YES      | Defines the task type [Type of task](#taskType)                                                              |
| executionTimestamp | number | NO       | With this parameter, the task is expected to be executed at the specified Unix epoch time (in milliseconds). |
| delay              | number | NO       | With this parameter, to delay task execution in millisecond.                                                 |

**Note:** If neither `executionTimestamp` nor `delay` is provided, the task will be executed immediately.


### taskType

| name      | description                                                                                                                                                                                                                                                                                                                 |
|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PUB_SUB   | This type is the basic schedule task, which you just publish payload in kafka topic `schedulerTopic` and  the payload will be published kafka topic `taskExecutionTopic` when its scheduled to be executed. <br/>**Note:** `schedulerTopic` and `taskExecutionTopic` are already configured in the application config file. |
| SHELL_CMD | In this type, your payload, which is a Linux command, will be executed. <br/>**Note:** If you expect to run any application, it must be pre-installed on the worker machine prior to task execution.                                                                                                                        |



# General Architecture

<p align="center">
  <img src="docs/diagram1.png" alt="General Architecture"/>
</p>
