from pyspark.sql import SparkSession, DataFrame, Window
from main.base import PySparkJobInterface
import pyspark.sql.functions as F


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        # TODO: put your code here
        ...

    def count_available_vaccines(self, vaccines: DataFrame) -> int:
        # TODO: put your code here
        ...

    def find_earliest_used_vaccine(self, vaccines: DataFrame) -> str:
        # TODO: put your code here
        ...

    def total_vaccinations_per_country(self, vaccines: DataFrame) -> DataFrame:
        # TODO: put your code here
        ...
