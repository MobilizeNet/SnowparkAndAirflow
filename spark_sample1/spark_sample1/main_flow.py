# coding=utf-8
from pyspark.sql import SparkSession, SQLContext

from pyspark.sql.functions import udf, col, month
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType
from pyspark.sql import DataFrame


from spark_sample1.write_movie_cast_crew import write_cast_crew
from spark_sample1.output_data import output_df 
from spark_sample1.read_file import read_file



def run():
    spark = SparkSession.builder \
      .master("local[*]") \
      .enableHiveSupport() \
      .getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    sc.setLogLevel("ERROR")
    print ("Init Started From main_flow")   
    print('Calling inside run method')
    path="/home/BlackDiamond/workspace/sample_data/credits.csv"

    #Read Input files and set dataframe
    cast_crew_df = read_file(path,sc)
    print("inside run2")
    #Calling module to write dataframe into hive table
    write_cast_crew(cast_crew_df)

    #print data from hive tables
    output_df(spark)
