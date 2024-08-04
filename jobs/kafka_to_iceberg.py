from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



appName = "KafkaToIcebergSparkJob"

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName(appName) \
    .getOrCreate()

# Configurazione dei parametri Kafka
kafka_bootstrap_servers = "broker:29092"
kafka_topic = "users_created"

# Definire lo schema per i dati JSON
schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("post_code", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("registered_date", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("picture", StringType(), True)
])

# Lettura dei dati dal topic Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data"))

# Espandi le colonne dal campo "data"
df = df.select(
    col("data.id").alias("id"),
    col("data.first_name").alias("first_name"),
    col("data.last_name").alias("last_name"),
    col("data.gender").alias("gender"),
    col("data.address").alias("address"),
    col("data.post_code").alias("post_code"),
    col("data.email").alias("email"),
    col("data.username").alias("username"),
    to_timestamp(col("data.dob")).alias("dob"),
    to_timestamp(col("data.registered_date")).alias("registered_date"),
    col("data.phone").alias("phone"),
    col("data.picture").alias("picture")
)

# Specifica il percorso di checkpoint nel bucket MinIO
checkpoint_location = f"s3a://jobs-support/checkpoints/{appName}"

query = df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable("nessie.users_created")

# Mostra i dati in streaming alla console
    # Attendere che il job finisca
query.awaitTermination()
