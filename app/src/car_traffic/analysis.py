from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, lead, unix_timestamp, expr, desc, rank
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType


class CarDataAnalyzer:
    """
    This class is responsible for analyzing the car traffic data using Spark.
    It reads the data from the specified file path, processes it, and provides methods to get
    the total number of cars, cars per day, top three half hours with most cars, and one and half hour period
    with least cars.
    """
    def __init__(self, spark:SparkSession, file_path:str):
        self.car_data = None
        # spark = SparkSession.builder.appName("CarDataAnalyzer").getOrCreate()
        self.spark = spark
        self.file_path = file_path
        self.car_data:DataFrame = None


    def read_car_data(self) -> None:
        """
        This function reads the car traffic data from the specified file path and processes
        it to create a Spark DataFrame.
        :return: None
        """
        spark_schema = StructType([StructField("traffic_date_time", TimestampType(), False),
                                   StructField("car_count", IntegerType(), False)])
        self.car_data = (self.spark.read
                         .option("delimiter", " ")
                         .option("header", "false")
                         .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
                         .schema(spark_schema)
                         .csv(self.file_path))
        self.car_data.createOrReplaceTempView("cars")


    def get_total_cars(self) -> int:
        """
        This function returns the total number of cars in the dataset.
        :return:
        """
        return self.car_data.agg({"car_count": "sum"}).collect()[0][0]

    def get_cars_per_day(self) -> DataFrame:
        """
        This function returns the number of cars per day, in the format "yyyy-MM-dd total_cars". This is achieved by
        grouping the data by date and summing the car counts for each date. The result is ordered by date.
        The Spark code uses SQL (as demonstration) to perform the necessary transformations and aggregations on the data.
        :return: Spark DataFrame with columns "daywise_car_count" containing the date and total cars for
        that date in the format "yyyy-MM-dd total_cars"
        """
        return self.spark.sql( "SELECT concat(traffic_date,' ', total_cars) as daywise_car_count FROM ( "
                              "SELECT date_format(traffic_date_time,'yyyy-MM-dd') as traffic_date, sum(car_count) as total_cars "
                              "FROM cars WHERE car_count > 0 "
                              "GROUP BY date_format(traffic_date_time,'yyyy-MM-dd') "
                              "ORDER BY traffic_date)")

    def get_top_three_half_hours_with_most_cars(self):
        """
        This function returns the top 3 half hours with most cars, in the same format as the input file. The Spark code
        uses SQL (as demonstration) to perform the necessary transformations and aggregations on the data.
        It ranks the half hours based on car count and selects the top 3.
        :return: Spark DataFrame with column "top_three_half_hours_with_most_cars" containing the top 3 half hours with
        most cars in the format "yyyy-MM-dd'T'HH:mm:ss car_count"
        """
        return self.spark.sql( "SELECT top_three_half_hours_with_most_cars FROM "
                               "(SELECT concat(date_format(traffic_date_time,\"yyyy-MM-dd'T'HH:mm:ss\"),' ', car_count) "
                               "as top_three_half_hours_with_most_cars, rank() over (order by car_count desc) as rank "
                               "FROM cars "
                               "order by car_count desc) "
                               "WHERE rank <= 3")


    def get_one_and_half_hour_period_with_least_cars(self) -> DataFrame:
        """
        This function returns the 1.5 hour period with least cars (i.e. 3 contiguous half hour records). The Spark code
        uses window functions to calculate the total number of cars in each 1.5 hour period (3 contiguous half
        hour records) and ranks them to find the period with the least cars.
        It filters the periods to ensure that they are exactly 1.5 hours long (i.e., the difference between the
        end time and start time is 90 minutes). The code is in native pyspark without SQL, to demonstrate the use of
        window functions and other transformations directly on the DataFrame.
        :return: Spark DataFrame with columns "start_time", "end_time", and "total_cars_in_90_minutes" containing the
        start time, end time, and total cars in the 1.5 hour period with least cars.
        """
        w = Window().orderBy("traffic_date_time")
        return (self.car_data.orderBy("traffic_date_time").withColumn("car_count_30_minutes", col("car_count"))
                .withColumn("car_count_60_minutes", lead("car_count",1).over(w))
                .withColumn("car_count_90_minutes", lead("car_count",2).over(w))
                .withColumn("end_time", lead("traffic_date_time",2).over(w) + expr("INTERVAL 30 MINUTES"))
                # .withColumn("start_time", col("traffic_date_time") - expr("INTERVAL 30 MINUTES") )
                .withColumnRenamed("traffic_date_time", "start_time")
                .filter((unix_timestamp(col("end_time")) - unix_timestamp(col("start_time")))/60 == 90)
                .withColumn("total_cars_in_90_minutes", col("car_count_30_minutes") + col("car_count_60_minutes") + col("car_count_90_minutes"))
                .orderBy("total_cars_in_90_minutes")
                .select(col("start_time"), col("end_time"), col("total_cars_in_90_minutes"))
                .withColumn("rank", rank().over(Window.orderBy("total_cars_in_90_minutes")))
                .filter(col("rank") == 1)
                .select("start_time", "end_time", "total_cars_in_90_minutes")
                )
