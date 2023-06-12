import unittest
from src.assignment_1.utils import SparkSession,unix_timestamp,from_unixtime,col,to_utc_timestamp,date_format,\
    trim,from_utc_timestamp,unix_timestamp,udf,to_date,StringType,IntegerType,\
    DateType,StructType,StructField,LongType,Row,time_stamp_format,\
    time_stamp_format,removing_spaces,extract_date,replace_null_values,\
    create_dataframe,create_new_column,filter_records

class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a SparkSession
        cls.spark = SparkSession.builder.master("local[1]").appName("Pyspark assignment").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_time_stamp_format(self):
        # Define test data
        data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", "0001")]
        schema = StructType([
            StructField("Product Name", StringType(), True),
            StructField("Issue Date", StringType(), True),
            StructField("Price", IntegerType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Productnumber", StringType(), True)])
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = time_stamp_format(df)
        expected_data=[("Washing Machine", "2022-04-01T05:25:33.000+0530", 20000, "Samsung", "India", "0001")]
        expected_schema=StructType([
            StructField("Product Name", StringType(), True),
            StructField("Issue Date", StringType(), True),
            StructField("Price", IntegerType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Productnumber", StringType(), True)])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(df2.collect()), sorted(result_df.collect()))

    def test_removing_spaces(self):
        data = [("Washing Machine", "1648770933000", 20000, " Samsung", "India", "0001"),
                ("Refrigerator ","2022-04-01T05:26:39.000+0530", 35000, " LG", None, "0002")
                ]
        schema = StructType([
            StructField("Product Name", StringType(), True),
            StructField("Issue Date", StringType(), True),
            StructField("Price", IntegerType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Productnumber", StringType(), True)
        ])

        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = removing_spaces(df)
        expected_brand = "Samsung"
        actual_brand = result_df.select("Brand").first()[0]
        self.assertEqual(expected_brand, actual_brand)
    def test_extract_date(self):
        data = [("Washing Machine", "2022-04-01T05:25:33.000+0530",20000,"Samsung","India","0001")]

        schema = StructType([
            StructField("Product Name", StringType(), True),
            StructField("Issue Date", StringType(), True),
            StructField("Price", IntegerType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Productnumber", StringType(), True)])
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = extract_date(df)
        expected_data = [
            ("Washing Machine", "2022-04-01T05:25:33.000+0530", 20000, "Samsung", "India", "0001", "2022-04-01")
            ]

        expected_schema = StructType([
            StructField("Product Name", StringType(),True),
            StructField("Issue Date", StringType(),True),
            StructField("Price", IntegerType(),True),
            StructField("Brand", StringType(),True),
            StructField("Country", StringType(),True),
            StructField("Productnumber", StringType(),True),
            StructField("Date", StringType()),True])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(df2.collect()), sorted(result_df.collect()))

    def test_replace_null_values(self):
        data = [("Washing Machine", "2022-04-01T05:25:33.000+0530", 20000, "Samsung", "India", "0001"), ("Refrigerator ","2022-04-01T05:26:39.000+0530", 35000, "LG", None, "0002")]

        schema = StructType([
            StructField("Product Name", StringType(), True),
            StructField("Issue Date", StringType(), True),
            StructField("Price", IntegerType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Productnumber", StringType(), True)])
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = replace_null_values(df)
        expected_data = [
            ("Washing Machine", "2022-04-01T05:25:33.000+0530", 20000, "Samsung", "India", "0001"),("Refrigerator ", "2022-04-01T05:26:39.000+0530", 35000, "LG"," ", "0002")]
        expected_schema = StructType([
            StructField("Product Name", StringType()),
            StructField("Issue Date", StringType()),
            StructField("Price", IntegerType()),
            StructField("Brand", StringType()),
            StructField("Country", StringType()),
            StructField("Productnumber", StringType())])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(df2.collect()), sorted(result_df.collect()))

    def test_replace_null_values(self):
        data = [("Washing Machine", "2022-04-01T05:25:33.000+0530", 20000, "Samsung", "India", "0001"), ("Refrigerator ","2022-04-01T05:26:39.000+0530", 35000, "LG", None, "0002")]

        schema = StructType([
            StructField("Product Name", StringType(), True),
            StructField("Issue Date", StringType(), True),
            StructField("Price", IntegerType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Productnumber", StringType(), True)])
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = replace_null_values(df)
        expected_data = [
            ("Washing Machine", "2022-04-01T05:25:33.000+0530", 20000, "Samsung", "India", "0001"),("Refrigerator ", "2022-04-01T05:26:39.000+0530", 35000, "LG"," ", "0002")]

        expected_schema = StructType([
            StructField("Product Name", StringType()),
            StructField("Issue Date", StringType()),
            StructField("Price", IntegerType()),
            StructField("Brand", StringType()),
            StructField("Country", StringType()),
            StructField("Productnumber", StringType())])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(df2.collect()), sorted(result_df.collect()))
    def create_new_df(self):
        data = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", "0001"),
                (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", "0002"),
                (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", "0003")]
        schema = StructType([
            StructField("SourceId", IntegerType(), True),
            StructField("TransactionNumber", IntegerType(), True),
            StructField("Language", StringType(), True),
            StructField("ModelNumber", IntegerType(), True),
            StructField("StartTime", StringType(), True),
            StructField("ProductNumber", StringType(), True)])
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = create_dataframe(df)
        expected_data=[(150711, 123456, "EN", 456789,"2021-12-27T08:20:29.842+0000" , "0001"),
                      (150439, 234567, "UK", 345678,"2021-12-27T08:22:42.445+0000" ,"0002")]
        expected_schema = StructType([
            StructField("SourceId", IntegerType(), True),
            StructField("TransactionNumber", IntegerType(), True),
            StructField("Language", StringType(), True),
            StructField("ModelNumber", IntegerType(),True),
            StructField("StartTime", StringType(),True),
            StructField("ProductNumber", StringType(),True)])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(df2.collect()), sorted(result_df.collect()))

    def test_create_new_column(self):
        data = [(150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', '0001')]

        schema = StructType([
            StructField("SourceId", IntegerType(), True),
            StructField("TransactionNumber", IntegerType(), True),
            StructField("Language", StringType(), True),
            StructField("ModelNumber", IntegerType(), True),
            StructField("StartTime", StringType(), True),
            StructField("ProductNumber", StringType(), True)
        ])

        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = create_new_column(df)
        expected_data = [((150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', '0001', 1640593229000))]
        expected_schema = StructType([
            StructField("SourceId", IntegerType(),True),
            StructField("TransactionNumber", IntegerType(),True),
            StructField("Language", StringType(),True),
            StructField("ModelNumber", IntegerType(),True),
            StructField("StartTime", StringType(),True),
            StructField("ProductNumber", StringType(),True),
            StructField("start_time_ms", LongType(),True)
        ])

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        self.assertEqual(expected_df.collect(), result_df.collect())


    def test_filter_records(self):
        data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", "0001", "150711", "123456", "EN", "456789", "2021-12-27T08:20:29.842+0000", "0001"),
                ("Refrigerator", 1648770999000, 35000, "LG", None, "0002", "150439", "234567", "UK", "345678", "2021-12-27T08:21:14.645+0000", "0002"),
                ("Air Cooler", 1648770948000, 45000, "Voltas", None, "0003", "150647", "345678", "ES", "234567", "2021-12-27T08:22:42.445+0000", "0003"),
                ("Washing Machine", 1648770933000, 20000, "Samsung", "India", "0001", "150711", "123456", "EN", "456789", "2021-12-27T08:20:29.842+0000", "0001")]
        schema = StructType([
                StructField("Product Name", StringType(), True),
                StructField("Issue Date",StringType(), True),
                StructField("Price", IntegerType(), True),
                StructField("Brand", StringType(), True),
                StructField("Country", StringType(), True),
                StructField("Productnumber", StringType(), True),
                StructField("SourceId", StringType(), True),
                StructField("TransactionNumber", StringType(), True),
                StructField("Language", StringType(), True),
                StructField("ModelNumber", StringType(), True),
                StructField("StartTime", StringType(), True),
                StructField("ProductNumber", StringType(), True)])
        df = self.spark.createDataFrame(data=data, schema=schema)
        result_df = filter_records(df)
        expected_data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", "0001", "150711", "123456", "EN", "456789", "2021-12-27T08:20:29.842+0000", "0001"),("Washing Machine", 1648770933000, 20000, "Samsung", "India", "0001", "150711", "123456", "EN", "456789", "2021-12-27T08:20:29.842+0000", "0001")]
        expected_schema = StructType([
                            StructField("Product Name", StringType(), True),
                            StructField("Issue Date", StringType(), True),
                            StructField("Price", IntegerType(), True),
                            StructField("Brand", StringType(), True),
                            StructField("Country", StringType(), True),
                            StructField("Productnumber", StringType(), True),
                            StructField("SourceId", StringType(), True),
                            StructField("TransactionNumber", StringType(), True),
                            StructField("Language", StringType(), True),
                            StructField("ModelNumber", StringType(), True),
                            StructField("StartTime", StringType(), True),
                            StructField("ProductNumber", StringType(), True)])
        df2 = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(df2.collect()), sorted(result_df.collect()))



if __name__ == '__main__':
    unittest.main()
