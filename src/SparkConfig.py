from pyspark.sql import SparkSession

def get_spark_instance():

    return SparkSession \
            .builder \
            .master("local") \
            .appName("transformed-data") \
            .getOrCreate()