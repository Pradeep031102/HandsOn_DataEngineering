from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, udf
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("MovieDataTransformations").getOrCreate()

# Load the Dataset
df = spark.read.csv("movies.csv", header=True, inferSchema=True)

# Filter Movies by Genre
sci_fi_movies = df.filter(col("genre") == "Sci-Fi")

# Top-Rated Movies
top_rated_movies = df.orderBy(col("rating").desc()).limit(3)

# Movies Released After 2010
movies_after_2010 = df.filter(col("date") > "2010-01-01")

# Calculate Average Box Office Collection by Genre
avg_box_office_by_genre = df.groupBy("genre").agg(avg("box_office").alias("avg_box_office"))

# Add a New Column for Box Office in Billions
def box_office_in_billions(box_office):
    return box_office / 1e9

box_office_udf = udf(box_office_in_billions, DoubleType())
df = df.withColumn("box_office_in_billions", box_office_udf(col("box_office")))

# Sort Movies by Box Office Collection
sorted_movies = df.orderBy(col("box_office").desc())

# Count the Number of Movies per Genre
count_movies_per_genre = df.groupBy("genre").count()

sci_fi_movies.show()
top_rated_movies.show()
movies_after_2010.show()
avg_box_office_by_genre.show()
df.show()
sorted_movies.show()
count_movies_per_genre.show()

spark.stop()
