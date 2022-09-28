from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext

from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType


def write_cast_crew(df):
    print("Calling inside write_movie_cast_crew")
    cast_json_schema = ArrayType(StructType([StructField("cast_id", IntegerType()), StructField("character", StringType()), StructField("credit_id", StringType()), StructField("gender", IntegerType()), StructField("id", IntegerType()), StructField("name", StringType()), StructField("order", IntegerType()), StructField("profile_path", StringType())]))

    crew_json_schema = ArrayType(StructType([StructField("credit_id", StringType()), StructField("department", StringType()), StructField("gender", IntegerType()), StructField("id", IntegerType()), StructField("job", StringType()), StructField("name", StringType()), StructField("profile_path", StringType())]))

    #z.show(df)
    df.show()
    print(f"Before {df.count()}")
    a_df = df.withColumn("cast", from_json(df.cast, cast_json_schema))
    a_df.select(col("cast"),size(col("cast")).alias("c")).filter(col("c") == 9).show(1000)
    #for i in range(1,26):
    #    c = a_df.select(size(col("cast")).alias("c")).filter(col("c") == i).count()
    #    print(f">>>>>>>{i} ")
    #print(f">>>>{c}")
    a_df.select(sum(size(col("cast")))).show()
    cast_converted_df = df.withColumn("cast", explode(from_json(df.cast, cast_json_schema)))

    print(f"After {cast_converted_df.count()}")
    #z.show(cast_converted_df)
    cast_converted_df.select(col("id").alias("movie_id"),
                            col("cast.cast_id").alias("cast_id"),
                            col("cast.character").alias("character"),
                            col("cast.credit_id").alias("credit_id"),
                            col("cast.gender").alias("gender"),
                            col("cast.id").alias("id"),
                            col("cast.name").alias("name"),
                            col("cast.profile_path").alias("profile_path")
                        ).write.format("parquet").mode("overwrite").saveAsTable("credit_movie_cast")
    crew_converted_df = df.withColumn("crew", explode(from_json(df.crew, crew_json_schema)))
    crew_converted_df.select(   col("id").alias("movie_id"),
                            col("crew.credit_id").alias("credit_id"),
                            col("crew.department").alias("department"),
                            col("crew.gender").alias("gender"),
                            col("crew.id").alias("id"),
                            col("crew.job").alias("job"),
                            col("crew.name").alias("name"),
                            col("crew.profile_path").alias("profile_path")
                        ).write.format("parquet").mode("overwrite").saveAsTable("credit_movie_crew")


