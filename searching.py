from pyspark import SparkContext, SparkConf

# Create a Spark context with a descriptive app name
conf = SparkConf().setAppName("SearchElement").setMaster("local")
sc = SparkContext(conf=conf)

# Define the data to be searched using a meaningful variable name
data_to_search = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Parallelize the data into an RDD
rdd = sc.parallelize(data_to_search)

# Define the search function with a clear purpose
def search_for_element(element):
   """Checks if the element is present within a collection."""
   return element == 5  # Change the search element as needed

# Map the search function across the RDD
result = rdd.map(search_for_element)

# Collect the results
search_results = result.collect()

# Print the search result clearly
if any(search_results):
   print("Element found in the dataset")
else:
   print("Element not found in the dataset")

# Stop the Spark context
sc.stop()