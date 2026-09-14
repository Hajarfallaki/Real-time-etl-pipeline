from pyspark.sql import SparkSession
from transformation import transform_transactions
from load import write_to_postgres


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

raw_df = (
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

    # Kafka stocke key et value sous forme de bytes.
    # On transforme "value" en texte JSON.
    .selectExpr("CAST(value AS STRING) AS value")
)


# ============================================================
# 3. Transformer (parsing + validation + enrichissement)
# ============================================================

transactions = transform_transactions(raw_df)


# ============================================================
# 4. Charger vers Postgres
# ============================================================

query = (
    transactions.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)


# ============================================================
# 5. Garder le programme actif
# ============================================================

query.awaitTermination()