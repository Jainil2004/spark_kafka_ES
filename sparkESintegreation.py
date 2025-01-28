from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToElasticsearch") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.10.1") \
    .getOrCreate()


# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "my_topic") \
    .load()

# Select and process the value (message) column
processed_df = kafka_df.selectExpr("CAST(value AS STRING) as message")

processed_df.printSchema()

es_query = processed_df.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", "my_index") \
    .option("es.net.ssl", "false") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

