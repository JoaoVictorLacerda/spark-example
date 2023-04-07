from SparkConfig import get_spark_instance
from pipeline.GetRaw import get_raw
from pipeline.GetTransformed import get_transformed
from pipeline.Transform import transform
from pipeline.WriteTransformedData import write_transformed_data


try:
    print("START")

    spark_session = get_spark_instance()

    #Pipeline
    get_raw(spark_session)
    dataframe_transformed = get_transformed(spark_session)
    new_transformed = transform(spark_session, dataframe_transformed)
    write_transformed_data(new_transformed)

    print("FINISH")
except Exception as e:
    print('Error when execute job', e)
    
