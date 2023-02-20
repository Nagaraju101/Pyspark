from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS ARILINE_DB")
    spark.catalog.setCurrentDatabase("ARILINE_DB")

    # flightTimeParquetDF.write \
    #                    .format("csv") \
    #                    .mode("overwrite") \
    #                    .partitionBy("ORIGIN", "OP_CARRIER") \
    #                    .saveAsTable("flight_data_table")

    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", True) \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .sortBy(("ORIGIN", "OP_CARRIER")) \
        .saveAsTable("flight_data_table")

    logger.info(spark.catalog.listTables("ARILINE_DB"))
