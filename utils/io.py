from pyspark.sql import DataFrame
from utils.spark_session import spark

def read_csv(path: str, header=True, inferSchema=True) -> DataFrame:
    return spark.read.csv(path, header=header, inferSchema=inferSchema)

def write_parquet(df: DataFrame, path: str, mode="overwrite", partitionBy=None):
    writer = df.write.mode(mode)
    if partitionBy:
        writer = writer.partitionBy(partitionBy)
    writer.parquet(path)
    print(f"Success: Data disimpan ke {path}")