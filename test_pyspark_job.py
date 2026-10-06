import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

# Force Spark to use the exact active Python executable for Driver and Worker
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark Session for testing."""
    return SparkSession.builder \
        .appName("PySpark-CI-Testing") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def test_clean_data(spark):
    # Sample dataset covering valid and edge cases
    data = [
        ("Alice", 100.0),    # Valid
        ("Bob", -10.0),      # Invalid (amount <= 0)
        ("Charlie", 0.0),    # Invalid (amount <= 0)
        (None, 50.0),        # Invalid (name is NULL)
        ("David", 200.0)     # Valid
    ]
    schema = ["name", "amount"]
    df = spark.createDataFrame(data, schema)

    # Execute transformation
    result_df = clean_data(df)
    results = result_df.collect()

    # 1. Verify row count (only Alice and David should remain)
    assert result_df.count() == 2

    # 2. Verify remaining names and filtered records
    remaining_names = [row["name"] for row in results]
    assert "Alice" in remaining_names
    assert "David" in remaining_names
    assert "Bob" not in remaining_names
    assert "Charlie" not in remaining_names
    assert None not in remaining_names

    # 3. Verify amount_with_tax calculation (1.20 multiplier)
    alice_row = next(row for row in results if row["name"] == "Alice")
    assert alice_row["amount_with_tax"] == pytest.approx(120.0)

    david_row = next(row for row in results if row["name"] == "David")
    assert david_row["amount_with_tax"] == pytest.approx(240.0)