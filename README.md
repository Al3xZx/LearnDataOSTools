# Project created to experiment with different Big Data technologies

## Currently using Dremio MinIO Spark Airflow Kafka (and Docker)

## Docker Setup

To run this project using Docker, follow these steps:

1. Clone this repository to your local machine.
2. Navigate to the directory containing the `docker-compose.yml` file.
3. Build and run the containers using Docker Compose:

```bash
docker-compose up -d --build
```
This command will start the necessary services defined in your docker-compose.yml, such as Airflow webserver, scheduler, Spark master, and worker containers.

## dashboard url

* AirFlow http://localhost:8080/
* Spark Console http://localhost:9090/
* Kafka confluent control center http://localhost:9021/
* Dremio http://localhost:9047/
* Minio http://localhost:9001/
