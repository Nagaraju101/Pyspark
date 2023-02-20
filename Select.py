from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("local[3]").setAppName("AnythingForMyFamily")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    df = spark.read.option("header", True).option("inferSchema", True)

    data  = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")
        ]
    columns = ["firstname","lastname","country","state"]

    df = spark.createDataFrame(data=data, schema=columns)

    #df.show()

    # To display a specif columns
    #df.select("firstname", "lastname").show()
    #df.select(df.firstname, df.lastname).show()
    # df.select(df["firstname"],df["lastname"]).show()

    # To display all columns and we can choose number rows in show
    # df.select(*columns).show()
    # df.select([col for col in df.columns]).show(2)
    df.select("*").show(3)

    # Selects first 3 columns and top 3 rows
    df.select(df.columns[:3]).show(3)

    # Selects columns 2 to 4  and top 3 rows
    df.select(df.columns[2:4]).show(3)
