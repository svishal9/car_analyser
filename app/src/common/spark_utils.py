from pyspark.sql import SparkSession

def get_or_create_spark_session(app_name: str = "CarDataAnalyzer"):
    spark=SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark