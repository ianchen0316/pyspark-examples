from pyspark.sql import SparkSession 
import pyspark.sql.functions as F 

spark = SparkSession.builder.appName("word count").getOrCreate() 

spark.sparkContext.setLogLevel("WARN")

results = (
	spark.read.text("./data/gutenberg_books/1342-0.txt")
	.select(F.split(F.col("value"), " ").alias("line"))
	.select(F.explode(F.col("line")).alias("word"))
	.select(F.lower(F.col("word")).alias("word"))
	.select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
	.where(F.col("word") != "")
	.groupby(F.col("word"))
	.count() 
	) 

results.orderBy(F.col("count"), ascending=False).show(10)
results.coalesce(1).write.csv("./results/text_count/")