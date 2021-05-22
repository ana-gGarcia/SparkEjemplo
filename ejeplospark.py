import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sparkContext=spark.sparkContext
rdd=sparkContext.parallelize([1,2,3,4,5])
rddCollect = rdd.collect()
print("Numero de particiones: "+str(rdd.getNumPartitions()))
print("Acccion primer elemento: "+str(rdd.first()))
print(rddCollect)

emptyRDD = sparkContext.emptyRDD()
emptyRDD2 = rdd=sparkContext.parallelize([])

print("is Empty RDD : "+str(emptyRDD2.isEmpty()))