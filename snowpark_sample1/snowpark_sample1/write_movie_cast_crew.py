from __future__ import print_function
from snowflake.snowpark import Session

from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *

from snowflake.snowpark.types import StringType, IntegerType, StructType, StructField, ArrayType

from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import PandasSeries, PandasDataFrame

cast_json_schema = {
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": [
    {
      "type": "object",
      "properties": {
        "cast_id": {
          "type": "integer"
        },
        "character": {
          "type": "string"
        },
        "credit_id": {
          "type": "string"
        },
        "gender": {
          "type": "integer"
        },
        "id": {
          "type": "integer"
        },
        "name": {
          "type": "string"
        },
        "order": {
          "type": "integer"
        },
        "profile_path": {
          "type": "string"
        }
      }
    }
  ]
}

crew_json_schema = {
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": [
    {
      "type": "object",
      "properties": {
        "credit_id": {
          "type": "string"
        },
        "department": {
          "type": "string"
        },
        "gender": {
          "type": "integer"
        },
        "id": {
          "type": "integer"
        },
        "job": {
          "type": "string"
        },
        "name": {
          "type": "string"
        },
        "profile_path": {
          "type": "string"
        }
      }
    }
  ]
}

def write_cast_crew(df):
    print("Calling inside write_movie_cast_crew")
    # we will use try_parse_json here
    def from_json(x):
        return call_builtin("try_parse_json",replace(x, "'",'"'))
    cast_converted_df = df.withColumn("cast",from_json(df.cast))
    cast_converted_df = cast_converted_df.join_table_function("flatten","cast").drop("CAST","KEY","PATH","SEQ","INDEX","THIS").withColumn("CAST",col("VALUE"))
    cast_converted_df.select(   
                            col("id").alias("movie_id"),
                            col("cast")["cast_id"].alias("cast_id"),
                            col("cast")["character"].alias("character"),
                            col("cast")["credit_id"].alias("credit_id"),
                            col("cast")["gender"].alias("gender"),
                            col("cast")["id"].alias("id"),
                            col("cast")["name"].alias("name"),
                            col("cast")["profile_path"].alias("profile_path")
                        ).write.mode("overwrite").saveAsTable("credit_movie_cast")
    crew_converted_df = df.withColumn("crew", from_json(df.crew))
    crew_converted_df = crew_converted_df.join_table_function("flatten","crew").drop("CREW","KEY","PATH","SEQ","INDEX","THIS").withColumn("CREW",col("VALUE"))
    crew_converted_df.select(   col("id").alias("movie_id"),
                            col("crew")["credit_id"].alias("credit_id"),
                            col("crew")["department"].alias("department"),
                            col("crew")["gender"].alias("gender"),
                            col("crew")["id"].alias("id"),
                            col("crew")["job"].alias("job"),
                            col("crew")["name"].alias("name"),
                            col("crew")["profile_path"].alias("profile_path")
                        ).write.mode("overwrite").saveAsTable("credit_movie_crew")


