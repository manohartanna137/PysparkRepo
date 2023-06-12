from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import unix_timestamp,from_unixtime,col,to_utc_timestamp,date_format,trim,from_utc_timestamp,unix_timestamp,row_number,avg,sum,min,max
from functools import reduce
from pyspark.sql.types import StringType,IntegerType,DateType,StructType,StructField

#creating a spark session
def spark_session():
    spark=SparkSession.builder.getOrCreate()
    return spark

#creating a dataframe
def dataFrame_creation(spark):
    data=[("James","Sales",3000),
      ("Michael","Sales",4600),
      ("Robert","Sales",4100),
      ("Maria","Finance",3000),
      ("Raman","Finance",3000),
      ("Scott","Finance",3300),
      ("Jen","Finance",3900),
      ("Jeff","Marketing",3000),
      ("Kumar","Marketing",2000)]
    schema=StructType([StructField("EmpName",StringType(),True),
                   StructField("Department",StringType(),True),
                   StructField("Salary",IntegerType(),True)])
    df=spark.createDataFrame(data=data, schema=schema)
    return df
def dept_wise_row_first(df):
    window1=Window.partitionBy("Department").orderBy("Salary")
    df1=df.withColumn("row",row_number().over(window1)).filter(col("row")==1).drop("row")
    return df1


def dept_wise_high_sal(df):
    window2=Window.partitionBy("Department").orderBy(col("Salary").desc())
    df1=df.withColumn("row",row_number().over(window2)).filter(col("row")==1).drop("row")
    return df1

def low_avg_high_totalsal(df):
    window3=Window.partitionBy("Department").orderBy("Salary")
    real_data=Window.partitionBy("Department")
    df1=df.withColumn("row",row_number().over(window3))\
            .withColumn("Average",avg(col("Salary")).over(real_data))\
            .withColumn("highest_salary",max(col("Salary")).over(real_data))\
            .withColumn("lowest_salary",min(col("Salary")).over(real_data))\
            .withColumn("total_salary",sum(col("Salary")).over(real_data))\
            .where(col("row")==1).drop("row").select("Department","Average","highest_salary","lowest_salary","total_salary")
    return df1