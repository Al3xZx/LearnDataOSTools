import airflow
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Recupera le credenziali MinIO dalla connessione di Airflow
conn = BaseHook.get_connection('minio_conn')
minio_access_key = conn.login
minio_secret_key = conn.password
minio_endpoint = conn.host

#nessie
# Full url of the Nessie API endpoint to nessie
url = "http://nessie:19120/api/v1"
# Where to store nessie tables
full_path_to_warehouse = ...
# The ref or context that nessie will operate on (if different from default branch).
# Can be the name of a Nessie branch or tag name.
ref = "main"
# Nessie authentication type (NONE, BEARER, OAUTH2 or AWS)
auth_type = "NONE"

with DAG(
        dag_id="users_stream_kafka_to_iceberg",
        default_args={
            "owner": "Alessandro Molinaro",
            "start_date": airflow.utils.dates.days_ago(1)
        },
        schedule_interval=None
) as dag: streaming_task = SparkSubmitOperator(
    task_id="users_stream_kafka_to_iceberg_job",
    conn_id="spark-conn",
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,'
             'org.apache.hadoop:hadoop-aws:3.3.4,'
             'com.amazonaws:aws-java-sdk-bundle:1.12.767,'
             'org.apache.iceberg:iceberg-spark3-runtime:0.13.2,'
             'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.94.4',
    application="jobs/kafka_to_iceberg.py",
    conf={
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        'spark.hadoop.fs.s3a.endpoint': minio_endpoint,
        'spark.hadoop.fs.s3a.access.key': minio_access_key,
        'spark.hadoop.fs.s3a.secret.key': minio_secret_key,
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        # 'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkCatalog',
        # 'spark.sql.catalog.spark_catalog.type': 'hive',
        # 'spark.sql.catalog.spark_catalog.warehouse': 's3a://dremio'

        "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        "spark.sql.catalog.nessie.uri": url,
        "spark.sql.catalog.nessie.ref": ref,
        "spark.sql.catalog.nessie.authentication.type": auth_type,
        "spark.sql.catalog.nessie.warehouse": 's3://nessie-test',


        # This is mandate config on spark session to use AWS S3
        "com.amazonaws.services.s3.enableV4": "true",
        "fs.AbstractFileSystem.s3a.impl": "org.apache.hadoop.fs.s3a.S3A",

    },

)



