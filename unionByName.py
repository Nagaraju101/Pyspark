from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    data = [("James", 34), ("Micheal",56),("Robert", 30), ("Maria", 24)]
    df = spark.createDataFrame(data=data, schema=["name", "age"])
    df.show()

    data2 = [(34, "James"), (45, "Maria"), (45, "Jen"), (34, "Jeff")]
    df2 = spark.createDataFrame(data=data2, schema=["age", "name"])
    df2.show()

    df3 = df.unionByName(df2)
    df3.show(truncate=False)
##################################################################################################
#UnionByName with different number of Columns

    df4 = spark.createDataFrame([[5,2,6]], ["col1", "col2", "col3"])
    df5 = spark.createDataFrame([[6,7,3]], ["col2", "col3", "col4"])

    df6 = df4.unionByName(df5, allowMissingColumns=True)
    df6.show()

