from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    arrayData = [
        ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
        ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
        ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
        ('Washington', None, None),
        ('Jefferson', ['1', '2'], {})]
    columns = ["name", "known_languages", "properties"]

    df = spark.createDataFrame(data=arrayData , schema=columns)
    df.printSchema()

    df2 = df.select(df.name, explode(df.known_languages))
    df2.show()