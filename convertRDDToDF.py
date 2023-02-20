from pyspark.sql import SparkSession


from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("ConvertRDDToDF") \
            .getOrCreate()

    logger = Log4j(spark)


    dept = [("Finance", 10), ("Marketing", 20), ("Sale", 30), ("IT", 40)]

    rdd = spark.sparkContext.parallelize(dept)

    deptCols = ["dept_name", "dept_id"]
    df1 = rdd.toDF(deptCols)
    df1.printSchema()
    df1.show()