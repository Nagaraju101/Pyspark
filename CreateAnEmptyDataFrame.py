from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName(("EmptyDataFrame")) \
            .getOrCreate()

    logger = Log4j(spark)

    schema = StructType\
            ([
            StructField('firstname', StringType()),
            StructField("middlename", StringType()),
            StructField("lastname", StringType())
            ])
    df1 = spark.createDataFrame([], schema)
    df1.show()


