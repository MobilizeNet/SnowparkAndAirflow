from pyspark.sql import SparkSession, SQLContext



from pyspark.sql.functions import udf, col, month
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType
from pyspark.sql import DataFrame
def output_df(spark):
    print("calling inside output data")
    print("Total movie cast")
    spark.sql("SELECT count(*) FROM credit_movie_cast").show()
    print("Total movie crew")
    spark.sql("SELECT count(*) FROM credit_movie_crew").show()
