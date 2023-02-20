from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("AnythingForMyFamily") \
        .getOrCreate()

    logger = Log4j(spark)

    dept = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("IT", 40)
            ]
    columns = ["Dept_name", "Dept_id"]

    deptDF = spark.createDataFrame(data=dept, schema=columns)
    deptDF.show()

    dataCollect = deptDF.collect()
    logger.info(dataCollect[1][0])
    print(dataCollect)

    for row in dataCollect:
        logger.info(row["Dept_name"] + "::"+str(row["Dept_id"]))


