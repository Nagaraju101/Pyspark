from pyspark.sql import SparkSession

from lib.logger import Log4j


if __name__ == "__main__":

    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("ConvertDFtoPANDAS") \
            .getOrCreate()

    logger = Log4j(spark)

    data = [("James", "", "Smith", "36636", "M", 60000),
            ("Michael", "Rose", "", "40288", "M", 70000),
            ("Robert", "", "Williams", "42114", "", 400000),
            ("Maria", "Anne", "Jones", "39192", "F", 500000),
            ("Jen", "Mary", "Brown", "", "F", 0)]

    columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]

    df1 = spark.createDataFrame(data = data, schema=columns)
    df1.printSchema()
    df1.show(truncate=False)
    #df2 = df1.toPandas()




