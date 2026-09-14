def write_to_postgres(batch_df, batch_id):
    """Ecrit un micro-batch transforme dans la table Postgres."""

    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/etl_db")
        .option("dbtable", "transactions")
        .option("user", "etl_user")
        .option("password", "etl_password")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
