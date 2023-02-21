from pyspark.sql import SparkSession
from lib.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    '''
        PySpark map (map()) is an RDD transformation that is used to apply the transformation function (lambda) on every element of RDD/DataFrame 
        and returns a new RDD.
        RDD map() transformation is used to apply any complex operations like adding a column, updating a column, transforming the data e.t.c,
        the output of map transformations would always have the same number of records as input.
    '''
    data = ["Project", "Gutenberg’s", "Alice’s", "Adventures",
            "in", "Wonderland", "Project", "Gutenberg’s", "Adventures",
            "in", "Wonderland", "Project", "Gutenberg’s"]
    # PySpark map() Example with RDD
    rdd = spark.sparkContext.parallelize(data)

    rdd2 = rdd.map(lambda x : (x,1))
    for element in rdd2.collect():
        print(element)

#     PySpark map() Example with DataFrame

    data = [('James', 'Smith', 'M', 30),
            ('Anna', 'Rose', 'F', 41),
            ('Robert', 'Williams', 'M', 62),
            ]

    columns = ["firstname", "lastname", "gender", "salary"]

    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)

#   Refering columns by index
    rdd2 = df.rdd.map(lambda x : (x[0]+" "+ x[1],  x[2], x[3]*2))
    df2 = rdd2.toDF(["name", "gender", "New Salary"])
    df2.show()


