# Lendo Excel e salvando no CosmosDB
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import os
import sys

#recebendo alguns parametros
file = sys.argv[1] 
collection = sys.argv[2]

spark= SparkSession \
    .builder \
    .appName("Processa Excel") \
    .getOrCreate()

excelFile = pd.ExcelFile(file)

#sheets list
sheets = excelFile.sheet_names

#Lista de DataFrames
list_df = ['df_' + s for s in sheets]

#Add FILE_PATH
for a in list_df:
  print("a")
  exec(a + "['FILE_PATH'] = '" + file + "'")
  exec("colList" + a + " = list(" + a + ")" )

from pyspark.sql.types import *

#schema e transformando para spark
dfSpark = []

#convertendo pandas para Spark
for l in list_df:
  exec(l + "Schema = StructType([ StructField(i, StringType(), True) for i in colList" + l + "]) ")
  exec(l + "Spark = spark.createDataFrame(" + l + " , " + l + "Schema)"  )
  dfSpark.append(l + "Spark")

#carregando configs
writeConfig = {
 "uri" : os.environ['DATABASE_URL'],
 "Collection" : collection
}  

for j in dfSpark:
  exec(j + ".write.format('com.mongodb.spark.sql.DefaultSource').mode('append').options(**writeConfig).save()")