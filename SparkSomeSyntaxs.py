#Some Imp Initial Commands 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegralType,StringType,FloatType
from pyspark.sql import functions as f 
from datetime import date, time, datetime, timedelta
import configparser
from pyspark.sql import Window

spark = SparkSession.Builder.appName("MyApp").getOrCreate()
df = spark.read.parquet("/Path")
df = df.repartition("key_column")
df.show(10)
df.printSchema()
datetoday = datetime.now()


#1. Selecting Columns

SQL: SELECT column1, column2 FROM table
DataFrame API:
df.select("column1", "column2")


#2. Filtering Rows
SQL: SELECT * FROM table WHERE column1 > 10
DataFrame API:
df.filter(df["column1"] > 10)

#3. Column Aliasing
SQL: SELECT column1 AS col1 FROM table
DataFrame API:
df.select(df["column1"].alias("col1"))

#4. Aggregations
SQL: SELECT COUNT(*), AVG(column1) FROM table
DataFrame API:
from pyspark.sql import functions as F
df.agg(F.count("*"), F.avg("column1"))


#5. Group By
SQL: SELECT column1, SUM(column2) FROM table GROUP BY column1
DataFrame API:
df.groupBy("column1").agg(F.sum("column2"))

#6. Ordering (Sorting)
SQL: SELECT * FROM table ORDER BY column1 DESC
DataFrame API:
df.orderBy(df["column1"].desc())

#7. Joins
SQL: SELECT * FROM table1 JOIN table2 ON table1.id = table2.id
DataFrame API:
df1.join(df2, df1["id"] == df2["id"], "inner")  # "inner" can be replaced with "left", "right", "outer"
or
df = df1.join(df2, on="department_id", how="inner")


#8. Window Functions
SQL: ROW_NUMBER() OVER (PARTITION BY column1 ORDER BY column2 DESC)
DataFrame API:
from pyspark.sql.window import Window
window_spec = Window.partitionBy("column1").orderBy(df["column2"].desc())
df.withColumn("row_number", F.row_number().over(window_spec))


#9. Pivoting Data
SQL: Complex multi-step SQL queries for pivoting
DataFrame API:
df.groupBy("year").pivot("category").sum("sales")

#10. Handling Null Values (COALESCE)
SQL: SELECT COALESCE(column1, column2) FROM table
DataFrame API:
df.withColumn("new_column", F.coalesce(df["column1"], df["column2"]))

#11. Distinct Rows
SQL: SELECT DISTINCT column1, column2 FROM table
DataFrame API:
df.select("column1", "column2").distinct()

#12. Conditional Statements (CASE WHEN)
SQL: SELECT CASE WHEN column1 > 10 THEN 'High' ELSE 'Low' END AS label FROM table
DataFrame API:
df.withColumn("label", F.when(df["column1"] > 10, "High").otherwise("Low"))

#13. Union of DataFrames
SQL: SELECT * FROM table1 UNION ALL SELECT * FROM table2
DataFrame API:
df1.union(df2)





















