#!/usr/bin/env python3

#HUGO LE GAYRARD

import time, pandas
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession


#conf = SparkConf().setAppName("mytest").setMaster("local[*]")
#sc = SparkContext(conf=conf)

#print (dir(sc),"/n")


#spark = SparkSession.builder.appName("Test_Parquet").master("local[*]").getOrCreate()

#parquetDF = spark.read.csv("data.csv")

#parquetDF.coalesce(1).write.mode("overwrite").parquet("Parquet")


sc = SparkSession.builder.appName("job-1").master("local[*]").getOrCreate()

communes_df = sc.read.option("header",True).option("delimiter", ";").csv("/home/hugo/Communes.csv")
#communes_df.show()
#communes_df.printSchema()
#print(type(communes_df))

df = communes_df.select("DEPCOM","PTOT")
df.show()



cp_df = sc.read.option("header",True).option("delimiter", ";").csv("/home/hugo/code-insee-postaux-geoflar.csv")
#cp_df.show()
#cp_df.printSchema()
df1 = cp_df.select("CODE INSEE","Code Dept","geom_x_y")
df1.show()



poste_synop_df = sc.read.option("header",True).option("delimiter", ";").csv("/home/hugo/postesSynop.txt")
#poste_synop_df.show()
#poste_synop_df.printSchema()
df2 = poste_synop_df.select("ID","Latitude","Longitude")
df2.show()


synop_df = sc.read.option("header",True).option("delimiter", ";").csv("/home/hugo/synop.2020120512.txt")
#synop_df.show()
#synop_df.printSchema()
df3 = synop_df.select("t","numer_sta","date")
df3 = df3.withColumn("temperateur",df3.t -273.15)
df3 = df3.withColumnRenamed("numer_sta","ID")


df_synop = df3.join(df3, df3.ID == df2.ID)
df_synop = df_synop.drop(df2.ID)

df_synop.show()


#df3.show()








