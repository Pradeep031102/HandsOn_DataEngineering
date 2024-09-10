from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, avg

# Initialize
spark = SparkSession.builder \
    .appName("EmployeeSalaryETL") \
    .getOrCreate()

# 1. Extract--data from a CSV file
csv_file_path = "path/to/employee_data.csv"  # Replace with the actual path to your CSV file
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# 2. Transform:
# a. Filter: Only include employees aged 30 and above
filtered_df = df.filter(col("age") >= 30)

# b. Add New Column: Calculate a 10% bonus on the current salary
transformed_df = filtered_df.withColumn(
    "salary_with_bonus", col("salary") * 1.10
)

# c. Aggregation: Compute the average salary for each gender
gender_avg_salary_df = transformed_df.groupBy("gender").agg(
    avg("salary").alias("average_salary")
)

# 3. Load: Save the transformed data to a Parquet file
output_parquet_path = "path/to/transformed_employee_data.parquet"  # Replace with the desired output path
transformed_df.write.parquet(output_parquet_path, mode="overwrite")

# Also save the gender-based average salary data to another Parquet file
gender_avg_salary_output_path = "path/to/gender_avg_salary.parquet"  # Replace with the desired output path
gender_avg_salary_df.write.parquet(gender_avg_salary_output_path, mode="overwrite")

# Stop the SparkSession
spark.stop()
