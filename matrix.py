from pyspark import SparkContext

sc = SparkContext("local", "MatrixVectorMultiplication")

matrix = [
    [2, 3, 4],
    [1, 0, -1],
    [5, 2, 3]
]
vector = [1, 2, 3]

broadcasted_vector = sc.broadcast(vector)

matrix_rdd = sc.parallelize(enumerate(matrix))

result = matrix_rdd.map(lambda row: (row[0], sum([x * y for x, y in zip(row[1], broadcasted_vector.value)])))

result_list = result.collect()
print("Matrix Vector Multiplication Result:")
for row_index, value in result_list:
    print(f"Row {row_index}: {value}")

sc.stop()