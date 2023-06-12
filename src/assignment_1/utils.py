from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp,from_unixtime,col,to_utc_timestamp,date_format,trim,from_utc_timestamp,unix_timestamp,udf,to_date
from pyspark.sql.types import StringType,IntegerType,DateType,StructType,StructField,LongType,Row

data_products=[("Washing Machine","1648770933000",20000,"Samsung","India","0001"),("Refrigerator ","1648770999000",35000," LG",None,"0002"),("Air Cooler","1648770948000",45000," Voltas",None,"0003")]
schema_products=StructType([StructField("Product Name",StringType(), nullable=True),
                   StructField("Issue Date",StringType(), nullable=True),
                   StructField("Price",IntegerType(), nullable=True),
                   StructField("Brand",StringType(), nullable=True),
                   StructField("Country",StringType(), nullable=True),
                   StructField("Productnumber",StringType(), nullable=True)])

def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Pyspark assignment").getOrCreate()
    return spark
def create_dataframe(spark,data,schema):
    df = spark.createDataFrame(data=data, schema=schema)
    return df

def time_stamp_format(df):
    df = df.withColumn("Issue Date", from_unixtime(col("Issue Date") / 1000))
    df = df.withColumn("Issue Date", date_format(col("Issue Date"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    return df

def extract_date(df):
    df1 = df.withColumn("Date", date_format(col("Issue Date"), "yyyy-MM-dd"))
    return df1

def removing_spaces(df1):
    a= df1.withColumn("Brand", trim(col("Brand")))
    return a

def replace_null_values(df1):
    b = df1.na.fill(" ", ["Country"])
    return b



data=[(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000","0001"),(150439,234567,"UK",345678,"2021-12-27T08:21:14.645+0000","0002"),(150647,345678,"ES",234567,"2021-12-27T08:22:42.445+0000","0003")]
schema= StructType([
    StructField("SourceId", IntegerType(), nullable=True),
    StructField("TransactionNumber", IntegerType(), nullable=True),
    StructField("Language", StringType(), nullable=True),
    StructField("ModelNumber", IntegerType(), nullable=True),
    StructField("StartTime", StringType(), nullable=True),
    StructField("ProductNumber", StringType(), nullable=True)])



def create_new_column(df_time):
    df_time = df_time.withColumn("start_time_ms",(unix_timestamp(col("StartTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) * 1000)
    return df_time



def join_two_df(df,df_time):
    df_join = df.join(df_time,df.Productnumber == df_time.ProductNumber,"inner")
    return df_join

def filter_records(df_join):
    df_en=df_join.filter(df_join.Language=="EN")
    return df_en










