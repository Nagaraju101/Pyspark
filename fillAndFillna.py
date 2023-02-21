from pyspark.sql import SparkSession
from lib.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythinForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)
    filePath = "dataSource/small_zipcode.csv"

    df = spark.read.option("header", True)\
        .option("inferSchema", True)\
        .csv(filePath)

    df.printSchema()
    df.show()
