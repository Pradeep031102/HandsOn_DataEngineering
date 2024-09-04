import pandas as pd
from datetime import datetime

# Sample sales data
data = {
    "TransactionID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "CustomerID": [101, 102, 103, 101, 104, 102, 103, 104, 101, 105],
    "ProductID": [501, 502, 501, 503, 504, 502, 503, 504, 501, 505],
    "Quantity": [2, 1, 4, 3, 1, 2, 5, 1, 2, 1],
    "Price": [150.0, 250.0, 150.0, 300.0, 450.0, 250.0, 300.0, 450.0, 150.0, 550.0],
    "Date": [
        datetime(2024, 9, 1),
        datetime(2024, 9, 1),
        datetime(2024, 9, 2),
        datetime(2024, 9, 2),
        datetime(2024, 9, 3),
        datetime(2024, 9, 3),
        datetime(2024, 9, 4),
        datetime(2024, 9, 4),
        datetime(2024, 9, 5),
        datetime(2024, 9, 5)
    ]
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file
df.to_csv('sales_data.csv', index=False)

print("Sample sales dataset has been created and saved as 'sales_data.csv'.")


from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Sales Dataset Analysis").getOrCreate()
# Load the CSV file into a PySpark DataFrame
sales_df = spark.read.csv('sales_data.csv', header=True, inferSchema=True)

# Show the first few rows of the DataFrame
sales_df.show()

# Print the schema of the DataFrame
sales_df.printSchema()

# Show the first few rows
print("The first 5 rows are:")
sales_df.show(5)

# Get summary statistics for numeric columns
print("The summary is:")
sales_df.describe(['Quantity', 'Price']).show()

#1
from pyspark.sql.functions import col
sales_df = sales_df.withColumn('TotalSales', col('Quantity') * col('Price'))
sales_df.show()
#2
total_sales_per_product = sales_df.groupBy('ProductID').sum('TotalSales')
total_sales_per_product = total_sales_per_product.withColumnRenamed('sum(TotalSales)', 'TotalSales')
total_sales_per_product.show()

#3
top_selling_product = total_sales_per_product.orderBy(col('TotalSales').desc()).limit(1)
print(f"Top-Selling Product: ProductID = {top_selling_product['ProductID']}, Total Sales = {top_selling_product['TotalSales']}")

#4
total_sales_by_date = sales_df.groupBy('Date').sum('TotalSales')
total_sales_by_date = total_sales_by_date.withColumnRenamed('sum(TotalSales)', 'TotalSales')
total_sales_by_date.show()
#5
high_value_transactions = sales_df.filter(col('TotalSales') > 500)
high_value_transactions.show()

#1
repeat_customers = sales_df.groupBy('CustomerID').count().filter(col('count') > 1)
repeat_customers.show()

#2
avg_price_per_product = sales_df.groupBy('ProductID').avg('Price')
avg_price_per_product = avg_price_per_product.withColumnRenamed('avg(Price)', 'AveragePrice')
avg_price_per_product.show()
