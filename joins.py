from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AnythingForMyFamily") \
            .getOrCreate()

    logger = Log4j(spark)

    emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
           (2, "Rose", 1, "2010", "20", "M", 4000),
           (3, "Williams", 1, "2010", "10", "M", 1000),
           (4, "Jones", 2, "2005", "10", "F", 2000),
           (5, "Brown", 2, "2010", "40", "", -1),
           (6, "Brown", 2, "2010", "50", "", -1)
           ]
    empColumns = ["emp_id", "name", "superior_emp_id", "year_joined",
                  "emp_dept_id", "gender", "salary"]

    dept = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("IT", 40)
            ]
    deptColumns = ["dept_name", "dept_id"]

    empDf = spark.createDataFrame(data=emp, schema=empColumns)
    empDf.show()

    deptDf = spark.createDataFrame(data=dept, schema= deptColumns)
    deptDf.show()

    #InnerJoin
    innerJoinDF = empDf.join(deptDf, empDf.emp_dept_id == deptDf.dept_id,"inner")
    innerJoinDF.show()

    #Full Join
    fullJoinDF = empDf.join(deptDf, empDf.emp_dept_id == deptDf.dept_id, "outer")
    fullJoinDF.show()

    fullJoinDF2 = empDf.join(deptDf, empDf.emp_dept_id == deptDf.dept_id, "full")
    fullJoinDF2.show()

    fullJoinDf3 = empDf.join(deptDf, empDf.emp_dept_id == deptDf.dept_id,"fullouter")
    fullJoinDf3.show()