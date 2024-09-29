import pyspark as spark
# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F

# spark = SparkSession.builder \
# 			.master("local") \
# 			.appName("First PySpark") \
# 			.getOrCreate()

# df = spark.read \
# 	.csv("hdfs://localhost:9000/user/phatt/lab04/input/data01.tsv", 
#       sep=r"\t", header=True, inferSchema=True)

# df.select("Cases - cumulative total", 
#     df["WHO Region"].like("South-East Asia")) \
#     .show()

sc = spark.SparkContext(master="local", 
                        appName="First PySpark")

to_numeric = lambda x: float(x.replace(",",""))

def foo(x: spark.RDD, key: str):
    x = x.strip().split("\t")
    return to_numeric(x[2]) \
        if x[1] == key else 0.0

key_region = "South-East Asia"
path = "hdfs://localhost:9000" + \
"/user/phatt/lab04/input/WHO-COVID-19-20210601-213841.tsv"

cumsum = sc.textFile(path) \
	.map(lambda x: foo(x, key_region)) \
    .sum()

print("Cumulative Case of %s: %f" % (key_region, cumsum))
