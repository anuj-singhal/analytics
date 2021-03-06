from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.types import DateType
from main.base import PySparkJobInterface


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        # Initialize Spark Session
        sparkSession = SparkSession.builder \
                      .appName("Covid19 Vaccination Progress") \
                      .getOrCreate()
        sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        return sparkSession

    def count_available_vaccines(self, vaccines: DataFrame) -> int:
        # count number of unique vaccines around the world
        num = vaccines.select('vaccines').dropDuplicates().count()
        return num

    def find_earliest_used_vaccine(self, vaccines: DataFrame) -> str:
        # earliest vaccine which has been used in the world

        # convert date to DateType
        vaccines = vaccines.withColumn('date', F.from_unixtime(F.unix_timestamp(vaccines['date'], 'd/M/yy')).cast(DateType()))
        # sort dataset based on date in ascending order to get the earliest vaccine information
        df_vaccine = vaccines.orderBy(F.col("date"), F.col('vaccines')).select("vaccines").collect()
        # get the first vaccine from the dataset
        earliestVaccine = df_vaccine[0].asDict()["vaccines"]
        return earliestVaccine


    def total_vaccinations_per_country(self, vaccines: DataFrame) -> DataFrame:
        # Get the total vaccine available for each country

        # filter total vaccinations where data not available
        vaccines = vaccines.filter(F.col('total_vaccinations') > 0)

        # Aggregate dataset based on country and get the sum of total vaccines
        df_total = vaccines.groupBy("country").agg(F.sum("total_vaccinations").alias('total_vaccinations'))
        return df_total
        
