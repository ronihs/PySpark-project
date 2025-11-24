from pyspark.sql import SparkSession

def get_spark(app_name="TitanicPipeline"):
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "true")
            # Windows FIX (WAJIB kalau tidak pakai winutils)
            .config("spark.hadoop.fs.permissions.enabled", "false")
            .config("spark.hadoop.fs.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .getOrCreate())

spark = get_spark()
