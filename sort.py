from pyspark.sql import SparkSession

# Create a Spark session with a descriptive name
spark = SparkSession.builder \
    .appName("SortData") \
    .getOrCreate()

# Define dummy input data with clear variable name
data = [
    "3\tApple",
    "1\tBanana",
    "2\tOrange",
    "4\tGrapes"
]

# Create RDD from dummy data using spark context
data_rdd = spark.sparkContext.parallelize(data)

# Sort the data based on the first element after splitting by tab
sorted_data = data_rdd.sortBy(lambda x: int(x.split('\t')[0]))  # Ensure numeric sort

# Collect and print the sorted data
sorted_results = sorted_data.collect()
for result in sorted_results:
    print(result)

# Stop the Spark session
spark.stop()