import findspark
import pyspark
import sys
findspark.init()

if len(sys.argv) != 3:
	raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]

def myMapFunc(x): # takes an input, provides an output pairing
	return (len(x), 1) #changed to lenth

def myReduceFunc(v1, v2): # Merge two values with a common key - operation must be assoc. and commut.
	return v1 + v2

sc = pyspark.SparkContext()
print("Spark Context initialized.")
# textFile --> take the address of a text file, return it as an RDD (hadoop dataset) of strings
lines = sc.textFile(sys.argv[1])

# Flatmap --> Apply a function to each element of the dataset, then flatten the result.
sentence = lines.flatMap(lambda line: line.split("\n"))
# wordCounts = words.map(myMapFunc).reduceByKey(myReduceFunc)
# Part II:  MapReduce basics: maps sentence lengths to the count of sentences with that length.
sentenceLengthCounts = sentence.map(myMapFunc).reduceByKey(myReduceFunc)


print("Operations complete.")
sentenceLengthCounts.saveAsTextFile(sys.argv[2])
print("Output saved as text file.")
