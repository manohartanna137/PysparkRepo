from src.assignment_sql_4.utils import spark_session,dataFrame_creation,dept_wise_row_first, dept_wise_high_sal,low_avg_high_totalsal

spark=spark_session()

df=dataFrame_creation(spark)
df.show(truncate=False)

df_row=dept_wise_row_first(df)
df_row.show(truncate=False)

df_sal_details=dept_wise_row_first(df)
df_sal_details.show()

df_low_avg=low_avg_high_totalsal(df)
df_low_avg.show(truncate=False)



