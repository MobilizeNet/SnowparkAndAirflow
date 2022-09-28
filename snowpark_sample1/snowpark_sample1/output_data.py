from snowflake.snowpark import Session



from snowflake.snowpark.functions import udf, col, month
from snowflake.snowpark.types import StringType, IntegerType, StructType, StructField, ArrayType
from snowflake.snowpark import DataFrame
def output_df(session):
    print("calling inside output data")
    print("Total movie cast")
    session.sql("SELECT count(*) FROM credit_movie_cast").show()
    print("Total movie crew")
    session.sql("SELECT count(*) FROM credit_movie_crew").show()
