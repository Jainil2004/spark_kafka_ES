# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

# Read data from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "my_topic") \
    .load() 

# Select the value (message) column and cast it to string
messages = kafka_df.selectExpr("CAST(value AS STRING)")

# Write messages to console
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()