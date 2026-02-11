from unittest.mock import MagicMock, patch

from pyspark import Row

from app.src.car_traffic.analysis import CarDataAnalyzer
from app.src.print_details.print_analysis import PrintAnalysis

mock_spark = MagicMock()
mock_file_path = 'dummy'

class TestPrintAnalysis:
    def test_print_total_number_of_cars(self, capsys, mocker):
        mock_get_total_cars = mocker.patch.object(CarDataAnalyzer, "get_total_cars")
        mock_get_total_cars.return_value = 123
        pa = PrintAnalysis(spark=mock_spark, file_path=mock_file_path)
        pa.print_total_number_of_cars()

        captured = capsys.readouterr()
        assert "Total number of cars: 123" in captured.out

    def test_print_cars_per_day(self, capsys, mocker):
        mock_get_cars_per_day = mocker.patch.object(CarDataAnalyzer, "get_cars_per_day")
        mock_get_cars_per_day.return_value.collect.return_value = [Row(daywise_car_count='2021-12-01 179'),
                                              Row(daywise_car_count='2021-12-05 81')]
        pa = PrintAnalysis(spark=mock_spark, file_path=mock_file_path)
        pa.print_cars_per_day()

        captured = capsys.readouterr()
        assert "Cars per day:" in captured.out
        assert "2021-12-01 179" in captured.out
        assert "2021-12-05 81" in captured.out

    def test_print_top_three_half_hours_with_most_cars(self, capsys, mocker):
        mock_get_top_half_hours = mocker.patch.object(CarDataAnalyzer, "get_top_three_half_hours_with_most_cars")
        mock_get_top_half_hours.return_value.collect.return_value = [Row(top_three_half_hours_with_most_cars='2021-12-01T07:30:00 46'),
                                              Row(top_three_half_hours_with_most_cars='2021-12-01T08:00:00 42')]
        pa = PrintAnalysis(spark=mock_spark, file_path=mock_file_path)
        pa.print_top_three_half_hours_with_most_cars()

        captured = capsys.readouterr()
        assert "Top three half hours with most cars:" in captured.out
        assert "2021-12-01T07:30:00 46" in captured.out
        assert "2021-12-01T08:00:00 42" in captured.out


    def test_one_and_half_hour_period_with_least_cars(self, capsys, mocker):
        mock_get_least_cars_period = mocker.patch.object(CarDataAnalyzer, "get_one_and_half_hour_period_with_least_cars")
        mock_get_least_cars_period.return_value.collect.return_value = \
            [Row(start_time='2021-12-01T05:00:00', end_time='2021-12-01T06:30:00', total_cars_in_90_minutes=31)]
        pa = PrintAnalysis(spark=mock_spark, file_path=mock_file_path)
        pa.print_one_and_half_hour_period_with_least_cars()

        captured = capsys.readouterr()
        assert "One and half hour period with least cars:" in captured.out
        assert "2021-12-01T05:00:00" in captured.out
        assert "2021-12-01T06:30:00" in captured.out
        assert "31" in captured.out
