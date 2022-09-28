# coding=utf-8 
from snowflake.snowpark import Session



from snowflake.snowpark.functions import udf, col, month
from snowflake.snowpark.functions import split
from snowflake.snowpark.types import StringType, IntegerType, StructType, StructField, ArrayType
from snowflake.snowpark import DataFrame


def read_file(path, sc):
    '''
    path: file directory path
    '''
    print("Calling inside read file ")
    #file must only contain one json object per row to work with spark
    sqlContext = sc
    schema = StructType([ \
      StructField("cast",StringType(),True), \
      StructField("crew",StringType(),True), \
      StructField("id",StringType(),True)])
    sc.file.put(path, "@mystage", overwrite=True)
    df = sc.read.options({"field_delimiter": ",", "skip_header": 1,"field_optionally_enclosed_by":'"'}).schema(schema).csv("@mystage/credits.csv")
    return df