from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("First Spark DataFrame").getOrCreate()

# Path for Google Colab: "lab05/input/WHO-COVID-19-20210601-213841.tsv"
# Path for HDFS (resuse input in lab04): 

df = spark.read.csv("hdfs://localhost:9000/user/phatt/"+
                    "lab04/input/WHO-COVID-19-20210601-213841.tsv",
                    sep="\t", header=True)

"""# Finding ASEAN Cases"""

keyRegion = "South-East Asia"
# Only filter the Cases ... cells that Region == keyRegion and cast it
# to decimal

asian = df.filter(df["WHO Region"] == keyRegion) \
    .select(
        "Name",
        F.regexp_replace(df["Cases - cumulative total"], ",", "") \
        .cast(DecimalType(15, 3)) \
        .alias("Cumulative Cases of Asian")
    )

"""## Sum of cumulative total cases among ASEAN"""

asian.agg(F.sum(asian["Cumulative Cases of Asian"]) \
          .alias("Sum of cumulative total cases among ASEAN")) \
          .show()

"""## Maximum number of cumulative total cases among ASEAN"""

# Find min cumulation -> join to Asian with "Cumlat..." -> select "Name"
asian.agg(F.max(asian["Cumulative Cases of Asian"]) \
          .alias("Cumulative Cases of Asian")) \
          .join(asian, "Cumulative Cases of Asian") \
          .select(asian["Name"] \
                .alias("Maximum number of cumulative total cases among ASEAN")) \
          .show(truncate=False)

# """## Top 3 countries with the lowest number of cumulative cases among ASEAN"""

window = Window.orderBy(F.asc("Cumulative Cases of Asian"))
ranked_asian = asian.withColumn("rank", F.rank().over(window))
ranked_asian.filter(ranked_asian["rank"] <= 3).show(truncate=False)