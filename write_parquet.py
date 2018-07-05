# coding=utf-8
from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

spark = SparkSession \
    .builder \
    .appName("Processa CSV dados Viagem") \
    .getOrCreate()

to_value=lambda v: float(v.replace(",", "."))
udf_to_value = F.udf(to_value, FloatType())

df=spark.read.csv("201609_Diarias_utf8.csv", header=True, sep="\t")

df2=df.withColumn("Valor", udf_to_value(df["Valor Pagamento"])) \
    .withColumn("DtPg", F.to_date(df["Data Pagamento"], format="dd/MM/yyyy"))

df3=df2.select(df2["Nome Órgão Superior"].alias("Orgao"),
    df2["Nome Função"].alias("Funcao"),
    df2["Nome Programa"].alias("Programa"),
    df2["Nome Favorecido"].alias("Favorecido"),
    df2["Valor"], df2["DtPg"])

df3.write.parquet("governo/viagens/2017/07")
