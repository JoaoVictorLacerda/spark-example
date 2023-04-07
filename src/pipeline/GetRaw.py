def get_raw(spark_session):

     # Com esse comando a abtração do spark utiliza o hadoop
     # para ler esse arquivo e devolve um dataframe para ser manipulado no projeto
     # como vamos utilizar uma view, não iremos retornar o dataframe
    dataframe_raw = spark_session \
        .read \
        .format("csv") \
        .load("/tmp/data/raw/*", header=True, inferSchema=True)
    
    dataframe_raw.createOrReplaceTempView("raw") # cria uma view para ser utilizada em comandos sql