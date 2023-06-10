from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType,IntegerType,DateType,StructType,StructField
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Pyspark assignment").getOrCreate()
    return spark
def create_product_dataframe(spark):
    data_pro = [
        ("banana", 1000, "USA"),
        ("carrots", 1500, "INDIA"),
        ("beans", 1600, "sweden"),
        ("orange", 2000, "UK"),
        ("orange", 2000, "UAE"),
        ("banana", 400, "CHINA"),
        ("carrots", 1200, "CHINA")
    ]
    schema_pro = StructType([
        StructField("product", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("country", StringType(), True)
    ])
    df_pro = spark.createDataFrame(data=data_pro, schema=schema_pro)
    return df_pro

def pivot_df(df_pro):
    pivot_df = df_pro.groupBy("product").pivot("country").sum("amount")
    return pivot_df

def unpivot_dataframe(pivot_df):
    unpivot_df = pivot_df.select("product", expr("stack(6, 'china', CHINA, 'india', INDIA, 'sweden', Sweden, 'uae', UAE, 'uk', UK, 'usa', USA) as (country, Total)")).where("Total is not null")
    return unpivot_df

