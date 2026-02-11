from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from app.src.car_traffic.analysis import CarDataAnalyzer


def assert_if_dataframes_are_identical(actual: DataFrame, expected: DataFrame):
    assert actual.subtract(expected).count() == 0
    assert expected.subtract(actual).count() == 0


class TestCarAnalyserData:

    def test_read_car_data(self, initialize_spark):
        """
        Following code will find the car_data.txt file in the data directory and read the data from it into a
        Spark DataFrame. It will then compare the actual DataFrame with the expected DataFrame created from the data
        provided by the fixture.
        :param initialize_spark: Obtained from the fixture defined in conftest.py, which initializes a SparkSession and
        provides the file path to the car_data.txt file
        :return: None
        """
        spark, file_path, data = initialize_spark
        car_analyser_data = CarDataAnalyzer(spark=spark,file_path=file_path)
        car_analyser_data.read_car_data()
        actual = car_analyser_data.car_data
        expected = spark.createDataFrame(data, ["traffic_date_time", "car_count"]) \
            .withColumn("traffic_date_time", col("traffic_date_time").cast("timestamp"))
        # assertDataFrameEqual(actual, expected)
        assert_if_dataframes_are_identical(actual, expected)

    def test_get_total_cars(self, initialize_spark):
        spark, file_path, _ = initialize_spark
        car_analyser_data = CarDataAnalyzer(spark=spark,file_path=file_path)
        car_analyser_data.read_car_data()
        actual = car_analyser_data.get_total_cars()
        expected = 398
        assert actual == expected

    def test_get_cars_per_day(self, initialize_spark):
        spark, file_path, _ = initialize_spark
        car_analyser_data = CarDataAnalyzer(spark=spark,file_path=file_path)
        car_analyser_data.read_car_data()
        actual = car_analyser_data.get_cars_per_day()
        data = [('2021-12-01 179',),
                         ('2021-12-05 81',),
                         ('2021-12-08 134',),
                         ('2021-12-09 4',)]
        expected = spark.createDataFrame(data, ["daywise_car_count"])
        assert_if_dataframes_are_identical(actual, expected)

    def test_get_top_three_half_hours_with_most_cars(self, initialize_spark):
        spark, file_path, _ = initialize_spark
        car_analyser_data = CarDataAnalyzer(spark=spark,file_path=file_path)
        car_analyser_data.read_car_data()
        actual = car_analyser_data.get_top_three_half_hours_with_most_cars()
        data = [('2021-12-01T07:30:00 46', ),
                         ('2021-12-01T08:00:00 42', ),
                         ('2021-12-08T18:00:00 33', )]
        expected = spark.createDataFrame(data, ["top_three_half_hours_with_most_cars"])
        assert_if_dataframes_are_identical(actual, expected)

    def test_get_top_three_half_hours_with_least_cars(self, initialize_spark):
        spark, file_path, _ = initialize_spark
        car_analyser_data = CarDataAnalyzer(spark=spark,file_path=file_path)
        car_analyser_data.read_car_data()
        actual = car_analyser_data.get_one_and_half_hour_period_with_least_cars()
        data = [('2021-12-01 05:00:00','2021-12-01 06:30:00',31 ),]
        expected = spark.createDataFrame(data, ["start_time", "end_time", "total_cars_in_90_minutes"])
        assert_if_dataframes_are_identical(actual, expected)


# write edge cases for the above tests, for example, when there are no cars in the data, when there are multiple half hours with the same number of cars, etc.
    def test_get_top_three_half_hours_with_most_cars_with_ties(self, initialize_spark):
        spark, file_path, _ = initialize_spark
        # adding more data to create ties in the top three half hours with most cars
        additional_data = [('2021-12-07T07:30:00', 46),
                           ('2021-12-02T08:00:00', 42),
                           ('2021-12-08T18:00:00', 43),
                           ('2021-12-09T10:00:00', 43),  # tie with 2021-12-08T18:00:00
                           ('2021-12-09T11:00:00', 43)]  # tie with 2021-12-08T18:00:00 and 2021-12-09T10:00:00
        car_analyser_data = CarDataAnalyzer(spark=spark,file_path=file_path)
        car_analyser_data.read_car_data()
        (car_analyser_data.car_data.union(spark.createDataFrame(additional_data, ["traffic_date_time", "car_count"]))
         .createOrReplaceTempView("cars"))
        actual = car_analyser_data.get_top_three_half_hours_with_most_cars()
        expected_data = [('2021-12-01T07:30:00 46', ),
                         ('2021-12-07T07:30:00 46', ),
                         ('2021-12-08T18:00:00 43', ),
                         ('2021-12-09T10:00:00 43', ),
                         ('2021-12-09T11:00:00 43', )]
        expected = spark.createDataFrame(expected_data, ["top_three_half_hours_with_most_cars"])
        assert_if_dataframes_are_identical(actual, expected)


    def test_get_top_three_half_hours_with_least_cars_with_ties(self, initialize_spark):
        spark, file_path, _ = initialize_spark
        # adding more data to create ties in the 1.5 hour period with least cars
        additional_data = [('2021-12-02 05:00:00', 25),
                           ('2021-12-02 05:30:00', 5),
                           ('2021-12-02 06:00:00', 1)]
        car_analyser_data = CarDataAnalyzer(spark=spark,file_path=file_path)
        car_analyser_data.read_car_data()
        car_analyser_data.car_data = car_analyser_data.car_data.union(spark.createDataFrame(additional_data, ["traffic_date_time", "car_count"]))
        car_analyser_data.car_data.show(100,truncate=False)
        actual = car_analyser_data.get_one_and_half_hour_period_with_least_cars()
        expected_data = [('2021-12-01 05:00:00','2021-12-01 06:30:00',31 ),
                         ('2021-12-02 05:00:00','2021-12-02 06:30:00',31 ),]
        expected = spark.createDataFrame(expected_data, ["start_time", "end_time", "total_cars_in_90_minutes"])
        assert_if_dataframes_are_identical(actual, expected)
