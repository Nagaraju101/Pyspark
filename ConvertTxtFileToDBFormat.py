from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnyThingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    # populatinDF = spark.read \
    #     .option("delimiter", "|") \
    #     .option("header", True) \
    #     .option('inferSchema', True) \
    #     .csv("dataSource/population.txt")
    #
    # populatinDF.show()

    peopleDf = spark.read \
              .json("dataSource/people.json")

    peopleDf.show()

    peopleDf.withColumn("New_Age_Column", col("age") +lit(1)).show()

    