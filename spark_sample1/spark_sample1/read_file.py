# coding=utf-8 
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession, SQLContext



from pyspark.sql.functions import udf, col, month
from pyspark.sql.functions import split, explode
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType
from pyspark.sql import DataFrame


def read_file(path, sc):
    '''
    path: file directory path
    '''
    print("Calling inside read file ")
    #file must only contain one json object per row to work with spark
    sqlContext = SQLContext(sc)
    schema = StructType() \
      .add("cast",StringType(),True) \
      .add("crew",StringType(),True) \
      .add("id",StringType(),True)
      
    df = sqlContext.read.load(path,format="csv", sep=",", schema=schema, header="true")
    return df