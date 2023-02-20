from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    data = [("James", "Sales", "NY", 90000, 34, 10000),
                  ("Michael", "Sales", "NY", 86000, 56, 20000),
                  ("Robert", "Sales", "CA", 81000, 30, 23000),
                  ("Maria", "Finance", "CA", 90000, 24, 23000)
                  ]

    columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
    df = spark.createDataFrame(data=data, schema=columns)

    data2 = [("James", "Sales", "NY", 90000, 34, 10000),
                   ("Maria", "Finance", "CA", 90000, 24, 23000),
                   ("Jen", "Finance", "NY", 79000, 53, 15000),
                   ("Jeff", "Marketing", "CA", 80000, 25, 18000),
                   ("Kumar", "Marketing", "NY", 91000, 50, 21000)
                   ]
    columns2 = ["employee_name", "department", "state", "salary", "age", "bonus"]

    df2 = spark.createDataFrame(data=data2, schema=columns2)

    unionDF = df.union(df2).distinct()
    unionDF.show()

    unionAllDf = df.unionAll(df2)
    unionAllDf.show()
