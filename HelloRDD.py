import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from lib.logger import Log4j

surveryRecord = namedtuple("SurveyRecord", ["age", "Gender", "Country", "State"])
if __name__ == "__main__":
    conf = SparkConf() \
    .setMaster("local[3") \
    .setAppName("HelloRDD")

    #sc = SparkContext(conf=conf)



    spark = SparkSession \
            .builder \
            .config(conf = conf) \
            .appName("HelloRDD") \
            .getOrCreate()

    sc = spark.sparkContext
    logger = Log4j(spark)

    if len(sys.argv) !=2:
        logger.error("Usage: HelloRDD <filename>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line : line.replace('"', '').split(','))
    selectRDD = colsRDD.map(lambda cols: surveryRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter((lambda r: r.age <40))
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)
    colList = countRDD.collect()
    for x in colList:
        logger.info(x)

