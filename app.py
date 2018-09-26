from pyspark import sql

spark = (
    sql
        .SparkSession
        .builder
        .appName('kafka-spark-openshift-python')
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0')
        .getOrCreate()
)

records = spark \
        .readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'bones-brigade') \
        .load() \


query = records.selectExpr("CAST(value AS STRING)")\
        .writeStream \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .start()

query.awaitTermination()
