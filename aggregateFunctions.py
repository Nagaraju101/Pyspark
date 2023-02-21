from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, collect_list, collect_set

from lib.logger import Log4j



if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[2]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    data = [("James", "Sales", 3000),
                  ("Michael", "Sales", 4600),
                  ("Robert", "Sales", 4100),
                  ("Maria", "Finance", 3000),
                  ("James", "Sales", 3000),
                  ("Scott", "Finance", 3300),
                  ("Jen", "Finance", 3900),
                  ("Jeff", "Marketing", 3000),
                  ("Kumar", "Marketing", 2000),
                  ("Saif", "Sales", 4100)
                  ]
    schema = ["employee_name", "department", "salary"]

    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)

    print("approx_count_distinct: " + \
          str(df.select(approx_count_distinct("salary")).collect()[0][0]))

    print("Avg Salary : " + str(df.select(avg("salary")).collect()[0][0]))

    df.select(collect_list("salary")).show()

    df.select(collect_set("salary")).show()