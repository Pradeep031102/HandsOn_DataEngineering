from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .getOrCreate()

# Sample employee data
data = [
    (1, 'Arjun', 'IT', 75000),
    (2, 'Vijay', 'Finance', 85000),
    (3, 'Shalini', 'IT', 90000),
    (4, 'Sneha', 'HR', 50000),
    (5, 'Rahul', 'Finance', 60000),
    (6, 'Amit', 'IT', 55000)
]

# Define schema (columns)
columns = ['EmployeeID', 'EmployeeName', 'Department', 'Salary']

# Create DataFrame
employee_df = spark.createDataFrame(data, columns)

# Show the DataFrame
employee_df.show()

from pyspark.sql.functions import avg, col

# Task 1: Filter Employees by Salary
high_salary_df = employee_df.filter(col('Salary') > 60000)
high_salary_df.show()

# Task 2: Calculate the Average Salary by Department
average_salary_df = employee_df.groupBy('Department').agg(avg('Salary').alias('AverageSalary'))
average_salary_df.show()

# Task 3: Sort Employees by Salary
sorted_salary_df = employee_df.orderBy(col('Salary').desc())
sorted_salary_df.show()

# Task 4: Add a Bonus Column
employee_with_bonus_df = employee_df.withColumn('Bonus', col('Salary') * 0.10)
employee_with_bonus_df.show()
