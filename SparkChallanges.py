""""
Challenge 1: Filter and Aggregate Data
You have a dataset of sales transactions. Write code to:

Filter the data to include only transactions over $100.
Group by "category" and calculate the total and average sales for each category.
Sample Data:

plaintext
Copy code
+---------+----------+-------+
| category| item     | amount|
+---------+----------+-------+
|   Books | Notebook |   150 |
|   Books | Pen      |    50 |
| Clothing| Shirt    |   200 |
| Clothing| Pants    |   100 |
+---------+----------+-------+
Expected Output:

plaintext
Copy code
+---------+----------+-----------+
| category| total    | average   |
+---------+----------+-----------+
|   Books |   150    |   150.0   |
| Clothing|   300    |   150.0   |
+---------+----------+-----------+

"""
#OPTION1
# Filter rows where amount > 100
df = df.filter(df["amount"] > 100)

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("myData")

# Run SQL query to calculate total and average sales per category
result_df = spark.sql("""
    SELECT Category, 
           SUM(amount) AS total, 
           AVG(amount) AS average 
    FROM myData 
    GROUP BY Category
""")

# Show the result
result_df.show()



#OPTION2

from pyspark.sql import functions as F

# Filter rows where amount > 100
df = df.filter(df["amount"] > 100)

# Group by 'category' and calculate total and average sales
result_df = df.groupBy("category").agg(
    F.sum("amount").alias("total"),
    F.avg("amount").alias("average")
)

# Show the result
result_df.show()





"""
Challenge 2: Add a Column Based on Conditions
Given a DataFrame of employee data, create a new column called performance_level based on the following conditions:

"High" if score is greater than 90.
"Medium" if score is between 70 and 90.
"Low" if score is below 70.
Sample Data:

plaintext
Copy code
+--------+-------+
| name   | score |
+--------+-------+
| Alice  | 95    |
| Bob    | 88    |
| Cathy  | 72    |
| David  | 65    |
+--------+-------+
Expected Output:

plaintext
Copy code
+--------+-------+----------------+
| name   | score | performance_level |
+--------+-------+----------------+
| Alice  | 95    | High           |
| Bob    | 88    | Medium         |
| Cathy  | 72    | Medium         |
| David  | 65    | Low            |
+--------+-------+----------------+
"""

#OPTION1
# Register the DataFrame as a temporary view
df.createOrReplaceTempView("myData")

# Execute SQL query to categorize performance level based on score
result_df = spark.sql("""
    SELECT name, 
           score, 
           CASE 
               WHEN score > 90 THEN 'High'
               WHEN score BETWEEN 70 AND 90 THEN 'Medium'
               WHEN score < 70 THEN 'Low' 
           END AS performance_level
    FROM myData
""")

# Show the result
result_df.show()


#OPTION2
from pyspark.sql import functions as F

# Add a new column 'performance_level' based on conditions
result_df = df.withColumn(
    "performance_level",
    F.when(df["score"] > 90, "High")
     .when((df["score"] >= 70) & (df["score"] <= 90), "Medium")
     .otherwise("Low")
)

# Show the result
result_df.show()




"""
Challenge 3: Window Functions - Ranking and Running Total
Given a DataFrame of sales transactions with columns category, date, and amount, calculate:

The rank of each transaction within each category, ordered by amount descending.
A running total of sales within each category.
Sample Data:

plaintext
Copy code
+---------+----------+--------+
| category| date     | amount |
+---------+----------+--------+
|   Books |2024-01-01|   200  |
|   Books |2024-01-02|   150  |
| Clothing|2024-01-01|   300  |
| Clothing|2024-01-02|   100  |
+---------+----------+--------+
Expected Output:

plaintext
Copy code
+---------+----------+--------+----+--------------+
| category| date     | amount |rank| running_total|
+---------+----------+--------+----+--------------+
|   Books |2024-01-01|   200  |  1 |    200       |
|   Books |2024-01-02|   150  |  2 |    350       |
| Clothing|2024-01-01|   300  |  1 |    300       |
| Clothing|2024-01-02|   100  |  2 |    400       |
+---------+----------+--------+----+--------------+
"""

#Option1

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("myData")

# Execute SQL query with window functions
result_df = spark.sql("""
    SELECT 
        category,
        date, 
        amount, 
        RANK() OVER (PARTITION BY category ORDER BY amount DESC) AS rank,
        SUM(amount) OVER (PARTITION BY category ORDER BY amount DESC) AS running_total
    FROM myData
""")

# Show the result
result_df.show()




#Option2
from pyspark.sql import Window
from pyspark.sql import functions as F

# Define the window specification
window_spec = Window.partitionBy("category").orderBy(F.desc("amount"))

# Calculate rank and running total
result_df = df.withColumn("rank", F.rank().over(window_spec)) \
              .withColumn("running_total", F.sum("amount").over(window_spec))

# Show the result
result_df.show()


"""
Challenge 4: Join and Deduplicate
Given two DataFrames, one with employee data and one with department data, join them on department_id and remove duplicates based on the employee_id.

Employee Data:

plaintext
Copy code
+------------+----------+-------------+
| employee_id| name     | department_id|
+------------+----------+-------------+
|     1      | Alice    | 101         |
|     2      | Bob      | 102         |
|     1      | Alice    | 101         |
|     3      | Cathy    | 101         |
+------------+----------+-------------+
Department Data:

plaintext
Copy code
+-------------+---------------+
| department_id| department    |
+-------------+---------------+
|     101     | Sales         |
|     102     | HR            |
+-------------+---------------+
Expected Output:

plaintext
Copy code
+------------+----------+-------------+------------+
| employee_id| name     | department_id| department |
+------------+----------+-------------+------------+
|     1      | Alice    | 101         | Sales      |
|     2      | Bob      | 102         | HR         |
|     3      | Cathy    | 101         | Sales      |
+------------+----------+-------------+------------+
"""

#OPTION1
# Register the DataFrames as temporary views
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

# Execute the SQL query to perform the join
df = spark.sql("""
    SELECT * 
    FROM df1 a
    INNER JOIN df2 b  
    ON a.department_id = b.department_id
""")

# Drop duplicates based on 'employee_id'
df = df.dropDuplicates(["employee_id"])

# Show the result
df.show()


#OPTION2

# Perform an inner join on 'department_id' between df1 and df2
df = df1.join(df2, on="department_id", how="inner")

# Drop duplicates based on 'employee_id'
df = df.dropDuplicates(["employee_id"])

# Show the result
df.show()


"""
Challenge 5: Pivot Data
You have a DataFrame with monthly sales data, including columns for year, month, category, and sales. Pivot the data to show the total sales by category for each year.

Sample Data:

plaintext
Copy code
+------+-------+---------+-------+
| year | month | category| sales |
+------+-------+---------+-------+
| 2024 | Jan   | Books   | 500   |
| 2024 | Jan   | Clothing| 200   |
| 2024 | Feb   | Books   | 600   |
| 2024 | Feb   | Clothing| 300   |
+------+-------+---------+-------+
Expected Output:

plaintext
Copy code
+------+---------+----------+
| year | Books   | Clothing |
+------+---------+----------+
| 2024 | 1100    | 500      |
+------+---------+----------+
"""


#OPTION1
query = """
    SELECT year,
           SUM(CASE WHEN category = 'Books' THEN sales ELSE 0 END) AS Books,
           SUM(CASE WHEN category = 'Clothing' THEN sales ELSE 0 END) AS Clothing
    FROM monthly_sales
    GROUP BY year
"""
result_df = spark.sql(query)
result_df.show()



#OPTION2
result_df = df.groupBy("year").pivot("category").sum("sales")
result_df.show()
