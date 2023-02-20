from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master(("local[3]")) \
        .appName("AnythingForMyFamily") \
        .getOrCreate()

    logger = Log4j(spark)

    data = [("james", "sales", 3000),
            ("Micheal", "Sales", 4600),
            ("Maria", "Finance", 3000),
            ("James", "Sales", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Saif", "Sales", 4100),
            ("Saif", "Sales",5000)
            ]

    columns = ["employee_name", "department", "salary"]

    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    df.show(truncate=False)

    distinctDf = df.distinct()
    print("Distint Count : " + str(distinctDf.count()))
    distinctDf.show(truncate=False)

    df2 = df.dropDuplicates()
    df2.show(truncate=False)

