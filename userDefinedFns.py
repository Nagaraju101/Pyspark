from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from lib.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    columns = ["seqno", "name"]
    data = [("1", "john jones"),("2", "tracey smith"),("3", "amy sanders")]


    df = spark.createDataFrame(data=data, schema=columns)
    df.show()

# Creating a python function
    def convertCase(str):
        resStr = ""
        arr = str.split(" ")
        for x in arr:
            resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
        return resStr

# Convert Python Function to Pyspark UDF
    convertUDF = udf(lambda z : convertCase(z), StringType())  #StringType() by default, so, it's not necessaryt
    convertUDF1 = udf(lambda z : convertCase(z))

    df.select(col("seqno") , convertUDF(col("name")).alias("Name")).show()

    def upperCase(str):
        return str.upper()

# To convert to upper case
    upperCaseUDF = udf(lambda z : upperCase(z))
    df2 = df.withColumn("UpperCaseName", upperCaseUDF(col("Name")))
    df2.show()


