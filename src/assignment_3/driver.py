from src.assignment_3.utils import create_product_dataframe,pivot_df,unpivot_dataframe,spark_session

spark=spark_session()

df_pro=create_product_dataframe(spark)
df_pro.show()

df_pivot=pivot_df(df_pro)
df_pivot.show()

df_unpivot= unpivot_dataframe(df_pivot)
df_unpivot.show()

