from pyspark.sql import SparkSession


# 1. Créer la session Spark
spark = (
    SparkSession.builder
    .appName("KafkaTransactionStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# 2. Lire le topic Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
)


# 3. Kafka donne la donnée sous forme de bytes
#    On récupère uniquement la valeur du message
transactions = df.selectExpr("CAST(value AS STRING) AS value")


# 4. Afficher les transactions
query = (
    transactions.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)


# 5. Garder le streaming actif
query.awaitTermination()