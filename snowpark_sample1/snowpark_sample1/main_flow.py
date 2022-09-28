# coding=utf-8
from snowflake.snowpark import Session

from snowflake.snowpark.functions import udf, col, month
from snowflake.snowpark.types import StringType, IntegerType, StructType, StructField, ArrayType
from snowflake.snowpark import DataFrame


from snowpark_sample1.write_movie_cast_crew import write_cast_crew
from snowpark_sample1.output_data import output_df 
from snowpark_sample1.read_file import read_file

import os



def run(session):
   # Added a change to pass connection from Airflow
   if not session:
      connection_parameters = {
         "account": os.environ["SNOW_ACCOUNT"],
         "user": os.environ["SNOW_USER"],
         "password": os.environ["SNOW_PASSWORD"],
         "role": os.environ["SNOW_ROLE"],
         "warehouse": os.environ["SNOW_WAREHOUSE"],
         "database": os.environ["SNOW_DATABASE"],
         "schema": os.environ.get("SNOW_SCHEMA")
      }
      session = Session.builder.configs(connection_parameters).create()
   session.sql("CREATE STAGE if not exists mystage ").show()
   sc = session

   print ("Init Started From main_flow")   
   print('Calling inside run method')
   path="/home/BlackDiamond/workspace/sample_data/credits.csv"

   #Read Input files and set dataframe
   cast_crew_df = read_file(path,sc)
   print("inside run2")
   #Calling module to write dataframe into hive table
   write_cast_crew(cast_crew_df)

   #print data from hive tables
   output_df(session)
