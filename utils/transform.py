from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_extract, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType

def add_timestamp(df: DataFrame, col_name="processed_at") -> DataFrame:
    return df.withColumn(col_name, current_timestamp())

def select_columns(df: DataFrame, columns: list) -> DataFrame:
    return df.select(*columns)

def clean_age(df: DataFrame) -> DataFrame:
    return df.withColumn("Age", col("Age").cast(DoubleType()))

def extract_deck(df: DataFrame) -> DataFrame:
    return df.withColumn("Deck", regexp_extract(col("Cabin"), r"([A-Za-z])", 1))

def fill_missing_embarked(df: DataFrame) -> DataFrame:
    return df.fillna({"Embarked": "S"})  # paling sering

def create_family_size(df: DataFrame) -> DataFrame:
    return df.withColumn("FamilySize", col("SibSp") + col("Parch") + 1)

def create_is_alone(df: DataFrame) -> DataFrame:
    return df.withColumn("IsAlone", when(col("FamilySize") == 1, 1).otherwise(0))

def extract_title(df: DataFrame) -> DataFrame:
    return df.withColumn("Title", 
                         regexp_extract(col("Name"), r", (\w+\.)", 1))

def simplify_title(df: DataFrame) -> DataFrame:
    rare_titles = ["Lady", "Countess","Capt", "Col","Don", "Dr", 
                   "Major", "Rev", "Sir", "Jonkheer", "Dona"]
    return (df.withColumn("Title", 
            when(col("Title").isin(rare_titles), "Rare")
            .when(col("Title").isin(["Mlle","Ms"]), "Miss")
            .when(col("Title") == "Mme", "Mrs")
            .otherwise(col("Title"))
        ))