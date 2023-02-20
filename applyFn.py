
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, lit, col

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("AnythingForMyFamily") \
        .getOrCreate()

    logger = Log4j(spark)

    columns = ["Seqno", "Name"]
    data = [(1, "john jones"),(2 , "tracey simith"),(3, "amy sanders" )]

    DF = spark.createDataFrame(data=data, schema=columns)
    DF.printSchema()
    DF.show(truncate=False)

    DF2 = DF.withColumn("Upper_name", upper(DF.Name) )
    DF2.show()

    DF3 =  DF.withColumn("serialNo", col("Seqno")  +lit(1))
    DF3.show()


