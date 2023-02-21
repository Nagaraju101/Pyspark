from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number


from lib.logger import Log4j


def over(windowSpec):
    pass


if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[2]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    data = (("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100),
            ("Maria", "Finance", 3000),
            ("James", "Sales", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Saif", "Sales", 4100)
            )
    schema = ["employee_name", "department", "salary"]

    df = spark.createDataFrame(data=data, schema=schema)
    df.show()
    
    #ROW_NUMBER
    df.withColumn("row_number", row_number().over(windowSpec)).show()