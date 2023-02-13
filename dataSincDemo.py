from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("SparkSchemaDemo") \
            .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    # To get the dataFrame writer by using write method
    #
    # flightTimeParquetDF.write \
    #          .format("avro") \
    #          .mode("overwrite") \
    #          .option("path", "dataSink/avro") \
    #          .save()
    #
    # logger.info("Number of partition before :" + str(flightTimeParquetDF.rdd.getNumPartitions()))

    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info("Number of partitions : " + str(partitionedDF.rdd.getNumPartitions()))
    cnt = partitionedDF.groupBy(spark_partition_id()).count()
    cnt.show()

    flightTimeParquetDF.write \
                .format("json") \
                .mode("overwrite") \
                .option("path","dataSink/json") \
                .partitionBy("OP_CARRIER", "ORIGIN") \
                .option("maxRecordsPerFile", 10000) \
                .save()

