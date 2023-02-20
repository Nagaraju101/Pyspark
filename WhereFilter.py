from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType

from lib.logger import Log4j

if __name__ == "__main__":
        spark = SparkSession.builder \
                        .master("local[3]") \
                        .appName("AnythingForMyFamily") \
                        .getOrCreate()

        logger = Log4j(spark)


        data = [
                (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
                (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
                (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
                (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
                (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
                (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
               ]

        columns = StructType([
                  StructField('name', StructType([
                  StructField('firstname', StringType(), True),
                  StructField('middlename', StringType(), True),
                  StructField('lastname', StringType(), True)
                              ])),
                  StructField('languages', ArrayType(StringType()), True),
                  StructField('state', StringType(), True),
                  StructField('gender', StringType(), True)
                ])

        df = spark.createDataFrame(data = data , schema= columns)
        df.printSchema()
        df.show(truncate=False)

        df2 = df.filter(df.state == "NY")
        df2.show()

        df3 = df2.filter((df2.gender == "M"))
        df3.show()

           