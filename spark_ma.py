from pyspark.sql.window import Window
from pyspark.sql import functions

input_file="stockdata.csv"
ouput_file="movingAverageStocks.csv"
r=5

#sparkSession?

df = spark.read.csv("stockdata.csv", header=True, sep=";")
df = df.withColumn("price", df["price"].cast("double"))
df = df.withColumn("date", df["date"].cast("date"))
df = df.withColumn("number", df["number"].cast("int"))

window = Window.orderBy("number").partitionBy("symbol").rangeBetween(-r, 0)
df2 = df.withColumn('movingAverage', functions.avg("price").over(window))

df2.coalesce(1).write.csv(path="ouput_file", header="true", mode="overwrite", sep=";")