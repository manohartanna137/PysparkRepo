from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp,from_unixtime,col,to_utc_timestamp,date_format,trim,from_utc_timestamp,unix_timestamp,lit,transform_keys,expr
from functools import reduce
from pyspark.sql.types import StringType,IntegerType,DateType,StructType,StructField,MapType

def spark_session():
    spark = SparkSession.builder.appName("SQL Assignment").getOrCreate()
    return spark


def create_dataframe(spark):
    data = [
        ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "03011998", "M", 3000),
        ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "10111998", "M", 20000),
        ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "02012000", "M", 3000),
        ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "03011998", "F", 11000),
        ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "04101998", "M", 10000)
    ]

    schema = StructType([
        StructField("name", MapType(StringType(), StringType()), True),
        StructField("dob", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True)
    ])

    df = spark.createDataFrame(data=data, schema=schema)
    return df


def select_nested_columns(df):
    df_nested = df.select(col("name.firstname").alias("firstname"), col("name.lastname").alias("lastname"), "salary")
    return df_nested


def add_columns(df):
    df_col = df.withColumn("Country", lit("Ind")).withColumn("department", lit("Civil")).withColumn("age", lit(28))
    return df_col


def change_column(df):
    df_sal = df.withColumn("salary", col("salary") * 2)
    return df_sal


def change_datatype(df):
    df_datatype = df.withColumn("dob", col("dob").cast("String")).withColumn("salary", col("salary").cast("String"))
    return df_datatype


def add_new_column(df):
    df_new_column = df.withColumn("salary_new", col("salary") * 2)
    return df_new_column


def rename_nested_column(df):
    df_rename_column = df.withColumn('name', expr("map('firstposition', name['firstname'], 'middleposition', name['middlename'], 'lastposition', name['lastname'])"))
    return df_rename_column

def drop_columns(df):
    cols = ["dob", "salary"]
    df_drop = df.drop(*cols)
    return df_drop


def distinct_value(df):
    df_distinct = df.select("dob", "salary").distinct()
    return df_distinct















