from pyspark.sql import SparkSession

from app.src.car_traffic.analysis import CarDataAnalyzer


class PrintAnalysis(object):
    """
    This class is responsible for printing the results of the car traffic analysis.
    It uses the CarDataAnalyzer to perform the analysis and then prints the results in a readable format.
    """
    def __init__(self, spark:SparkSession, file_path:str) -> None:
        super().__init__()
        self.spark = spark
        self.file_path = file_path
        self.car_analysis = CarDataAnalyzer(self.spark, file_path)
        self.car_analysis.read_car_data()

    def print_total_number_of_cars(self) -> None:
        """
        Prints the total number of cars in the dataset.
        :return: None
        """
        total_cars = self.car_analysis.get_total_cars()
        print(f"\n\nTotal number of cars: {total_cars}")

    def print_cars_per_day(self) -> None:
        """
        Prints the number of cars per day in the dataset.
        :return: None
        """
        cars_per_day = self.car_analysis.get_cars_per_day()
        print("\n\nCars per day:")
        for row in cars_per_day.collect():
            print(row.daywise_car_count)

    def print_top_three_half_hours_with_most_cars(self) -> None:
        """
        Prints the top three half hours with the most cars in the dataset.
        :return: None
        """
        top_half_hours = self.car_analysis.get_top_three_half_hours_with_most_cars()
        print("\n\nTop three half hours with most cars:")
        for row in top_half_hours.collect():
            print(row.top_three_half_hours_with_most_cars)

    def print_one_and_half_hour_period_with_least_cars(self) -> None:
        """
        Prints the one and half hour period with the least cars in the dataset.
        :return: None
        """
        least_cars_period = self.car_analysis.get_one_and_half_hour_period_with_least_cars()
        print("\n\nOne and half hour period with least cars:")
        for row in least_cars_period.collect():
            print(row.start_time, row.end_time, row.total_cars_in_90_minutes)


