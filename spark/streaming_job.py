from pyspark.sql import SparkSession


# ============================================================
# 1. Créer la session Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("KafkaTransactionStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Lire les données depuis Kafka
# ============================================================

df = (
    spark.readStream
    .format("kafka")

    # Kafka accessible depuis le réseau Docker interne
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    )

    # Topic Kafka contenant les transactions
    .option(
        "subscribe",
        "transactions"
    )

    # Lire uniquement les nouveaux messages
    .option(
        "startingOffsets",
        "latest"
    )

    .load()
)


# ============================================================
# 3. Transformer la valeur Kafka
# ============================================================

transactions = df.selectExpr(
    "CAST(value AS STRING) AS value"
)


# ============================================================
# 4. Afficher les transactions reçues
# ============================================================

query = (
    transactions.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)


# ============================================================
# 5. Garder le programme actif
# ============================================================

query.awaitTermination()