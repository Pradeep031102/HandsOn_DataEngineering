from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Product Sales Analysis") \
    .getOrCreate()

# Sample data
products = [
    (1, "Laptop", "Electronics", 50000),
    (2, "Smartphone", "Electronics", 30000),
    (3, "Table", "Furniture", 15000),
    (4, "Chair", "Furniture", 5000),
    (5, "Headphones", "Electronics", 2000),
]

sales = [
    (1, 1, 2),
    (2, 2, 1),
    (3, 3, 3),
    (4, 1, 1),
    (5, 4, 5),
    (6, 2, 2),
    (7, 5, 10),
    (8, 3, 1),
]

# Define schema
product_columns = ["ProductID", "ProductName", "Category", "Price"]
sales_columns = ["SaleID", "ProductID", "Quantity"]

# Create DataFrames
product_df = spark.createDataFrame(products, schema=product_columns)
sales_df = spark.createDataFrame(sales, schema=sales_columns)

# 1. Join the DataFrames
joined_df = product_df.join(sales_df, on="ProductID")
joined_df.show()

# 2. Calculate Total Sales Value
total_sales_df = joined_df.withColumn(
    "TotalSalesValue", col("Price") * col("Quantity")
)
total_sales_df.show()

# 3. Find Total Sales for Each Product Category
category_sales_df = total_sales_df.groupBy("Category") \
    .sum("TotalSalesValue")
category_sales_df.show()

# 4. Identify the Top-Selling Product
top_selling_product_df = total_sales_df.groupBy("ProductID", "ProductName") \
    .agg(sum("TotalSalesValue")) \
    .orderBy(col("sum(TotalSalesValue)").desc()) \
    .limit(1)
top_selling_product_df.show()

# 5. Sort Products by Total Sales Value
product_sales_df = total_sales_df.groupBy("ProductID", "ProductName") \
    .agg(sum("TotalSalesValue")) \
    .orderBy(col("sum(TotalSalesValue)").desc())
product_sales_df.show()

# 6. Count the Number of Sales for Each Product
sales_count_df = sales_df.groupBy("ProductID") \
    .count() \
    .withColumnRenamed("count", "NumberOfSales")
product_sales_count_df = sales_count_df.join(product_df, on="ProductID")
product_sales_count_df.select("ProductName", "NumberOfSales").show()

# 7. Filter Products with Total Sales Value Greater Than ₹50,000
filtered_sales_df = product_sales_df.filter(col("sum(TotalSalesValue)") > 50000)
filtered_sales_df.show()
