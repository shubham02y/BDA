






from pyspark import SparkContext
from math import sqrt

# Define dummy input data
input_data = [
   'key1\t10',
   'key2\t20',
   'key1\t30',
   'key2\t40',
   'key1\t50',
   'key2\t60',
]

# Mapping function to extract key-value pairs
def map_func(line):
   key, value = line.split('\t')
   return key, float(value)

# Reducing function to calculate aggregations
def reduce_func(data):
   values = list(data)  # Collect values for efficient calculations
   mean_val = sum(values) / len(values)
   sum_val = sum(values)
   std_dev_val = sqrt(sum((x - mean_val)**2 for x in values) / (len(values) - 1)) if len(values) > 1 else 0
   return {
       'mean': mean_val,
       'sum': sum_val,
       'std_dev': std_dev_val
   }

if __name__ == '__main__':
   # Create a SparkContext
   sc = SparkContext('local', 'AggregationSpark')

   # Create an RDD from input data
   lines = sc.parallelize(input_data)

   # Map key-value pairs
   mapped = lines.map(map_func)

   # Group by key
   grouped = mapped.groupByKey()

   # Apply reduce function to each group
   result = grouped.mapValues(list).mapValues(reduce_func)

   # Collect and print results
   output = result.collect()
   for key, value in output:
       print(f'{key}\t{value}')

   # Stop the SparkContext
   sc.stop()