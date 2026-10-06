from pyspark.sql import functions as F

def clean_data(df):
    """
    Clean DataFrame by:
    1. Removing rows where amount <= 0
    2. Removing rows where name is NULL
    3. Adding 'amount_with_tax' calculated as amount * 1.20
    """
    return (
        df.filter((F.col("amount") > 0) & (F.col("name").isNotNull()))
          .withColumn("amount_with_tax", F.col("amount") * 1.20)
    )