import requests
import json
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    def getpor():
        r = requests.get("http://144.202.17.134:5000/sensores")
        return r
    
    data = getpor()

    json_rdd = spark.sparkContext.parallelize([data.text])
    df = spark.read.json(json_rdd)
    v1 = df.select("estado").filter(df.estado == 'Apagado')
    v2 = df.select("estado").filter(df.estado == 'IZQUIERDA')
    v3 = df.select("estado").filter(df.estado == 'DERECHA')
    c1 = v1.count()
    c2 = v2.count()
    c3 = v3.count()
    total  = c1 + c2 + c3
    piap = round((c1*100) /total)
    pizq = round((c2*100) /total)
    pder = round((c3*100) /total)
    print("-------------------------------")
    print(str("Porcentaje APAGADO"),piap,"%")
    print(str("Porcentaje IZQUIERDA"),pizq,"%")
    print(str("Porcentaje DERECHA"),pder,"%")
    spark.stop()