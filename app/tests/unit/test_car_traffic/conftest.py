from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from app.src.common.spark_utils import get_or_create_spark_session

gdp_environment = 'local'
etl_date = '07/05/2021'


# @pytest.fixture
def get_spark_session():
    return SparkSession.builder.appName("CarDataAnalyzer").getOrCreate()

def get_sample_data():
    return [
        ("2021-12-01T05:00:00", 5),
        ("2021-12-01T05:30:00", 12),
        ("2021-12-01T06:00:00", 14),
        ("2021-12-01T06:30:00", 15),
        ("2021-12-01T07:00:00", 25),
        ("2021-12-01T07:30:00", 46),
        ("2021-12-01T08:00:00", 42),
        ("2021-12-01T15:00:00", 9),
        ("2021-12-01T15:30:00", 11),
        ("2021-12-01T23:30:00", 0),
        ("2021-12-05T09:30:00", 18),
        ("2021-12-05T10:30:00", 15),
        ("2021-12-05T11:30:00", 7),
        ("2021-12-05T12:30:00", 6),
        ("2021-12-05T13:30:00", 9),
        ("2021-12-05T14:30:00", 11),
        ("2021-12-05T15:30:00", 15),
        ("2021-12-08T18:00:00", 33),
        ("2021-12-08T19:00:00", 28),
        ("2021-12-08T20:00:00", 25),
        ("2021-12-08T21:00:00", 21),
        ("2021-12-08T22:00:00", 16),
        ("2021-12-08T23:00:00", 11),
        ("2021-12-09T00:00:00", 4)
    ]

@pytest.fixture(scope='session')
def initialize_spark(request):
    """Create a single node Spark application."""
    print("starting spark connection")
    spark_session = get_or_create_spark_session()
    file_path = str(Path(__file__).parent.parent / "data" / "car_data.txt")
    yield spark_session, file_path, get_sample_data()
    print('tearing down')
    spark_session.stop()
