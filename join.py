from pyspark import SparkContext

# Create a SparkContext with a meaningful app name
sc = SparkContext("local", "JoinsDemo")

# Create RDDs for left and right datasets with clear names
left_dataset = sc.parallelize([(1, "A"), (2, "B"), (3, "C")])
right_dataset = sc.parallelize([(1, "X"), (2, "Y"), (4, "Z")])

# Perform map-side join
map_side_join = left_dataset.join(right_dataset)

# Perform reduce-side join
reduce_side_join = left_dataset.union(right_dataset).reduceByKey(lambda x, y: (x, y))

# Print the results with descriptive labels
print("Map-Side Join Results:", map_side_join.collect())
print("Reduce-Side Join Results:", reduce_side_join.collect())

# Stop the SparkContext
sc.stop()