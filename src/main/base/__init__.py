import abc
from pyspark.sql import SparkSession, DataFrame


class PySparkJobInterface(abc.ABC):

    def __init__(self):
        self.spark = self.init_spark_session()

    @abc.abstractmethod
    def init_spark_session(self) -> SparkSession:
        """Create spark session"""
        raise NotImplementedError

    def read_csv(self, input_path: str) -> DataFrame:
        return self.spark.read.options(header=True, inferSchema=True).csv(input_path)

    @abc.abstractmethod
    def count_available_vaccines(self, vaccines: DataFrame) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def find_earliest_used_vaccine(self, vaccines: DataFrame) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def total_vaccinations_per_country(self, vaccines: DataFrame) -> DataFrame:
        raise NotImplementedError

    def stop(self) -> None:
        self.spark.stop()
