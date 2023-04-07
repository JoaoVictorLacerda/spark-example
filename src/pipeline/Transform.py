from datetime import datetime
from pyspark.sql.functions import lit

def transform(spark_session, dataframe_transformed):

    #Nessa query podemos obter todos os usuários no arquivo raw, pegando o ddd deles através de uma função
    # substring. Essa função pega os primeiros dois dígitos do campo telephone e cria mais uma coluna com o nome ddd
    transformed_sql = '''
        SELECT *, SUBSTRING(telephone, 1, 2) as ddd 
        FROM raw 
    '''
    users_transformed = spark_session.sql(transformed_sql)

    #Aqui estou colocando nossa ultima coluna: dt. Utilizando a função lit do pyspark, é possível inserir um valor para
    #uma nova coluna do seu dataframe
    now = datetime.now()
    current_partition = f'{now.year:04d}-{now.month:02d}-{now.day:02d}-{now.hour}'
    transformed = users_transformed.withColumn("dt", lit(current_partition))

    #E por fim, estou juntando todos os dados que foram processados da raw com todos os dados da transformed
    #excluindo duplicações (essa exclusão sempre ocorrerá primeiro em dataframe_transformed)
    transformed = transformed.union(dataframe_transformed).dropDuplicates(["cpf"])
    return transformed