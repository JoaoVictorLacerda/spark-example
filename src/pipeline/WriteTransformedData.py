def write_transformed_data(dataframe_transformed):
    dataframe_transformed \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .format("csv") \
        .save("/tmp/data/sparkResult", header=True, inferSchema=True)