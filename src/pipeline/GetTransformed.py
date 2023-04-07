from pyspark.sql.functions import col,date_format
def get_transformed(spark_session):

     # Com esse comando a abtração do spark utiliza o hadoop
     # para ler esse arquivo e devolve um dataframe para ser manipulado no projeto
    dataframe_transformed = spark_session \
        .read \
        .format("csv") \
        .load("/tmp/data/transformed/*", header=True, inferSchema=True)
    
    #Aqui estou dizendo que a coluna dt, com o formato yyyy-MM-dd deverá ser considerada uma string
    transformed = dataframe_transformed.withColumn("dt", col("dt").cast("string"))
    transformed = transformed.withColumn("dt", date_format(col("dt"), "yyyy-MM-dd"))
    transformed = transformed.withColumn("dt", col("dt").cast("string"))

    transformed.createOrReplaceTempView("transformed") # cria uma view para ser utilizada em comandos sql
    return transformed