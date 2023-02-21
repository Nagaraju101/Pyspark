from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythingForMyfamily") \
            .getOrCreate()

    logger = Log4j(spark)

    columns = ["Seqno", "Name"]
    data = [(1, "john jones"),
            (2, "tracey smith"),
            (3, "amy sanders")
            ]

    df = spark.createDataFrame(data=data, schema=columns)
    df.show()

    def f(df):
        print(df.Seqno)

    df.foreach(f)