from src.assignment_2.utils import spark_session,create_dataframe,\
add_columns,select_nested_columns,change_column,change_datatype,\
    add_new_column,rename_nested_column,\
    drop_columns,distinct_value

spark = spark_session()

df=create_dataframe(spark)
# df.show(truncate=False)
#
# df_select=select_nested_columns(df)
# df_select.show(truncate=False)
#
# df_add_col=add_columns(df)
# df_add_col.show(truncate=False)
#
#
# df_change_col=change_column(df)
# df_change_col.show(truncate=False)
#
# df_change_dt=change_datatype(df)
# df_change_dt.show(truncate=False)
#
# df_nested_name=rename_nested_column(df)
# df_nested_name.show(truncate=False)
#
# df_drop=drop_columns(df)
# df_drop.show(truncate=False)

df_distinct=distinct_value(df)
df_distinct.show(truncate=False)
