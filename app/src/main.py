import argparse

from common.spark_utils import get_or_create_spark_session
from report.print_analysis import PrintAnalysis


def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', type=str, required=True, help='Path to the car data file')
    return parser.parse_args()

def main():
    spark = get_or_create_spark_session()
    args = arg_parser()
    file_path = args.file_path
    print_analysis = PrintAnalysis(spark, file_path)
    print_analysis.print_total_number_of_cars()
    print_analysis.print_cars_per_day()
    print_analysis.print_top_three_half_hours_with_most_cars()
    print_analysis.print_one_and_half_hour_period_with_least_cars()

if __name__ == "__main__":
    main()
