from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Advanced DataFrame Operations - Different Dataset") \
    .getOrCreate()

# Create two sample DataFrames for Product Sales
data1 = [
    (1, 'Product A', 'Electronics', 1200, '2022-05-10'),
    (2, 'Product B', 'Clothing', 500, '2022-07-15'),
    (3, 'Product C', 'Electronics', 1800, '2021-11-05')
]

data2 = [
    (4, 'Product D', 'Furniture', 3000, '2022-03-25'),
    (5, 'Product E', 'Clothing', 800, '2022-09-12'),
    (6, 'Product F', 'Electronics', 1500, '2021-10-19')
]

# Define schema (columns)
columns = ['ProductID', 'ProductName', 'Category', 'Price', 'SaleDate']

# Create DataFrames
sales_df1 = spark.createDataFrame(data1, columns)
sales_df2 = spark.createDataFrame(data2, columns)

# 1. Union of DataFrames (removing duplicates)
union_df = sales_df1.union(sales_df2).dropDuplicates()
union_df.show()

# 2. Union of DataFrames (including duplicates)
union_all_df = sales_df1.union(sales_df2)
union_all_df.show()

# 3. Rank products by price within their category
window_spec_rank = Window.partitionBy('Category').orderBy(F.desc('Price'))
ranked_df = union_df.withColumn('Rank', F.row_number().over(window_spec_rank))
ranked_df.show()

# 4. Calculate cumulative price per category
window_spec_cum_sum = Window.partitionBy('Category').orderBy('SaleDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)
cumulative_price_df = union_df.withColumn('CumulativePrice', F.sum('Price').over(window_spec_cum_sum))
cumulative_price_df.show()

# 5. Convert SaleDate from string to date type
sales_df_date = union_df.withColumn('SaleDate', F.to_date('SaleDate', 'yyyy-MM-dd'))
sales_df_date.show()

# 6. Calculate the number of days since each sale
current_date = datetime.now().strftime('%Y-%m-%d')
days_since_sale_df = sales_df_date.withColumn('DaysSinceSale', F.datediff(F.lit(current_date), 'SaleDate'))
days_since_sale_df.show()

# 7. Add a column for the next sale deadline
next_sale_deadline_df = sales_df_date.withColumn('NextSaleDeadline', F.date_add('SaleDate', 30))
next_sale_deadline_df.show()

# 8. Calculate total revenue and average price per category
revenue_avg_df = union_df.groupBy('Category').agg(
    F.sum('Price').alias('TotalRevenue'),
    F.avg('Price').alias('AveragePrice')
)
revenue_avg_df.show()

# 9. Convert all product names to lowercase
lowercase_names_df = union_df.withColumn('ProductNameLower', F.lower('ProductName'))
lowercase_names_df.show()
