
from src.assignment_1.utils import *
spark=spark_session()

df=create_dataframe(spark,data=data_products,schema=schema_products)
df.show(truncate=False)

df1=time_stamp_format(df)
df1.show(truncate=False)

df2=extract_date(df1)
df2.show()

df3=removing_spaces(df)
df3.show(truncate=False)

df4=replace_null_values(df1)
df4.show(truncate=False)

df_time=create_dataframe(spark,data=data,schema=schema)
df_time.show(truncate=False)

df_create=create_new_column(df_time)
df_create.show(truncate=False)

df_join=join_two_df(df,df_time)
df_join.show(truncate=False)

df_filter=filter_records(df_join)
df_filter.show(truncate=False)







