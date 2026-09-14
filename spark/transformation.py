from pyspark.sql.functions import col, from_json, when, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# Schéma attendu du JSON envoyé par producer.py
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("type", StringType()),
    StructField("status", StringType()),
    StructField("timestamp", StringType())
])


def parse_transactions(df):
    """Extrait et structure le JSON brut venant de Kafka."""

    parsed_df = df.select(
        from_json(
            col("value"),
            transaction_schema
        ).alias("data")
    )

    return parsed_df.select("data.*")


def transform_transactions(df):
    """Applique les règles de nettoyage et d'enrichissement métier."""

    parsed_df = parse_transactions(df)

    cleaned_df = (
        parsed_df
        # --- Validation : rejette les lignes incomplètes ou invalides ---
        .filter(col("transaction_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("amount") > 0)

        # --- Cast du timestamp texte (string ISO) en vrai TimestampType ---
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .drop("timestamp")

        # --- Enrichissement : catégorise le montant ---
        .withColumn(
            "amount_category",
            when(col("amount") < 100, "small")
            .when(col("amount") < 1000, "medium")
            .otherwise("large")
        )

        # --- Enrichissement : horodatage du traitement Spark ---
        .withColumn("processed_at", current_timestamp())
    )

    return cleaned_df