from pyspark.sql import SparkSession
from pyspark.sql.functions import upper

from lib.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    data = [("Java", 4000, 5), ("Python", 4600, 10), ("Scala", 4100, 15) , ("Scala", 4500, 15), ("PHP", 3000, 20)]

    columns = ["course_name", "course_fee", "discount"]

    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    df.show(truncate=False)

# Creating custom functions
    def to_upper_str_columns(df):
        return df.withColumn("course_name", upper(df.course_name))

    def reduce_price(df,reducedBy):
        return df.withColumn("new_course_fee", df.course_fee - reducedBy )

    def applyDiscount(df):
        return df.withColumn("dicount_fee", df.new_course_fee - (df.new_course_fee * df.discount)/100)

    df2 = df.transform(to_upper_str_columns) \
            .transform(reduce_price,1000) \
            .transform(applyDiscount)

    df2.show()


