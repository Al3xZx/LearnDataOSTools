# Big Data Pipeline Project

## Description

This project was created to experiment with various Big Data technologies, including Apache Kafka, Apache Spark, Apache Airflow, MinIO, Dremio, and Nessie. It uses Docker Compose to orchestrate all necessary services and manage a data pipeline that transforms and loads real-time data.

## Technologies Used

- **Apache Kafka**: Distributed messaging system for data streaming.
- **Apache Spark**: Distributed processing engine for transforming and loading data into Iceberg.
- **Apache Airflow**: Orchestration tool for managing and scheduling workflows.
- **MinIO**: S3-compatible storage for Spark checkpoint storage and Iceberg data.
- **Dremio**: Data lake engine for data management.
- **Nessie**: Data catalog for managing Iceberg tables.


## Docker Setup

To run this project using Docker, follow these steps:

1. **Clone the Repository**

   Clone the repository containing the `docker-compose.yml` file and other configuration files:

   ```bash
   git clone https://github.com/your-username/big-data-pipeline-project.git
   cd big-data-pipeline-project
   ```
   
2. **Build and Start Containers**

    Build and start the containers using Docker Compose:

    ```bash
    docker-compose up -d --build
    ```
   
## Airflow Configuration

After starting the containers, you'll need to manually create the following connections in Apache Airflow:

1. **Connection to MinIO**

   - **Connection ID**: `minio_conn`
   - **Connection Type**: `Generic`
   - **Host**: `http://minio:9000`
   - **Login**: `minioadmin`
   - **Password**: `minioadmin`

2. **Connection to Spark Cluster**

   - **Connection ID**: `spark-conn`
   - **Connection Type**: `Spark`
   - **Host**: `spark://spark-master`
   - **Port**: `7077`

## Dashboard URLs

- **Airflow**: [http://localhost:8080/](http://localhost:8080/)
- **Spark Console**: [http://localhost:9090/](http://localhost:9090/)
- **Kafka Confluent Control Center**: [http://localhost:9021/](http://localhost:9021/)
- **Dremio**: [http://localhost:9047/](http://localhost:9047/)
- **MinIO**: [http://localhost:9001/](http://localhost:9001/)
- **Nessie**: [http://localhost:19120/](http://localhost:19120/)

## File Descriptions

- **`docker-compose.yml`**: Configures Docker services, including Kafka, Spark, Airflow, MinIO, Dremio, and Nessie.
- **`requirements.txt`**: Contains additional Python dependencies required for the project.
- **`jobs/kafka_to_iceberg.py`**: Spark job for transforming data from Kafka to Iceberg.
- **`dags/streaming_dag.py`**: Airflow DAG for running the data streaming job.

## Monitoring and Debugging

- **Airflow**: Use the web interface to monitor and manage DAGs and task executions.
- **Spark**: Use the Spark UI available at `http://localhost:9090` to monitor Spark jobs.
- **Kafka**: Access the Confluent Control Center to monitor topics and consumers.
- **MinIO**: Use the MinIO console to manage buckets and data.
- **Dremio**: Use the Dremio interface to explore and manage data.
- **Nessie**: Use the Nessie API to interact with the Iceberg catalog.