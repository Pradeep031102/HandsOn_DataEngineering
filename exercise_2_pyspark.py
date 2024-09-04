from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KeyValueRDDExercise").getOrCreate()
sc = spark.sparkContext

sales_data = [
    ("ProductA", 100),
    ("ProductB", 150),
    ("ProductA", 200),
    ("ProductC", 300),
    ("ProductB", 250),
    ("ProductC", 100)
]

sales_rdd = sc.parallelize(sales_data)
print("Sales Data RDD:", sales_rdd.take(5))

grouped_data = sales_rdd.groupByKey()
print("Grouped Data:", grouped_data.collect())

total_sales = sales_rdd.reduceByKey(lambda x, y: x + y)
print("Total Sales by Product:", total_sales.collect())

sorted_sales = total_sales.sortBy(lambda x: x[1], ascending=False)
print("Sorted Sales by Product:", sorted_sales.collect())

high_sales = total_sales.filter(lambda x: x[1] > 200)
print("Products with Sales > 200:", high_sales.collect())

regional_sales_data = [
    ("ProductA", 50),
    ("ProductC", 150)
]

regional_sales_rdd = sc.parallelize(regional_sales_data)
combined_rdd = sales_rdd.union(regional_sales_rdd)
combined_total_sales = combined_rdd.reduceByKey(lambda x, y: x + y)
print("Combined Sales Data:", combined_total_sales.collect())

distinct_products_count = sales_rdd.map(lambda x: x[0]).distinct().count()
print("Number of Distinct Products:", distinct_products_count)

max_sales_product = total_sales.reduce(lambda a, b: a if a[1] > b[1] else b)
print("Product with Maximum Sales:", max_sales_product)

total_sales_count = total_sales.map(lambda x: (x[0], (x[1], 1)))
sum_sales_count = total_sales_count.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
average_sales = sum_sales_count.map(lambda x: (x[0], x[1][0] / x[1][1]))
print("Average Sales per Product:", average_sales.collect())

spark.stop()
