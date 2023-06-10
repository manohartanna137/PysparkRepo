from src.assignment_2.utils import spark_session,create_dataframe,\
add_columns,select_nested_columns,change_column,change_datatype,\
    add_new_column,rename_nested_column,\
    drop_columns,distinct_value

spark = spark_session()

df=create_dataframe(spark)
df.show()

df_select=select_nested_columns(df)
df_select.show()

df_add_col=add_columns(df)
df_add_col.show()

df_change_col=change_column(df)
df_change_col.show()

df_change_dt=change_datatype(df)
df_change_dt.show()

df_drop=drop_columns(df)
df_drop.show()

df_distinct=distinct_value(df)
df_distinct.show()
