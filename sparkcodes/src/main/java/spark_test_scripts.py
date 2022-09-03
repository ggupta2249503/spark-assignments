import pandas
import os
import pyspark
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import statistics
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import openpyxl
from functools import reduce
from pyspark.sql import DataFrame
#import spark.implicits._
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.functions import udf



def test_codes():
    
    spark = SparkSession.builder.master("local[1]").appName('test').getOrCreate()
    spark.catalog.clearCache()
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext
    #print(pandas.__version__)
    sqlContext = SQLContext(sc)
    lst=[("banana",2,"fruit"),("apple",8,"fruit"),("leek",2,"vegetable"),("cabbage",9,"vegetable"),("lettuce",10,"vegetable"),("kale",23,"vegetable")]
    cols=["item","purchases","category"]
    df=spark.createDataFrame(lst,cols)
    sqlContext.sql("drop table if exists table1")
    df.createTempView("table1")
    df1=sqlContext.sql("select category,sum(purchases) from table1 group by category")
    df2=df.join(df1,df.category==df1.category,"inner")
    df2.show()
    df3=sqlContext.sql("select item,purchases,category,sum(purchases) over(partition by category) as total_purchases from table1")
    df3.show()
    df4=sqlContext.sql("select item,purchases,category,lag(purchases) over(partition by category order by purchases asc) as lag_purchases from table1")
    df4.show()
    df5=sqlContext.sql("select item,purchases,category,lag(purchases) over(partition by category order by purchases asc) as lag_purchases from table1")
    df6=df5.withColumn("total_purchases",when(df5.lag_purchases.isNull(),df5.purchases).otherwise(df5.purchases+df5.lag_purchases))
    df6.show()
    lst1=[(1,"Name:Prashant;salary:1000;role:DE"),(2,"Name:Shrishti;age:27;org:Facebook;city:Bangalore")]
    cols1=["id","value"]
    dfnew=spark.createDataFrame(lst1,cols1)
    dfnew.show()
    dfnew1=dfnew.rdd.map(lambda row:(row[0],row[1].split(";"))).toDF(["a","b"])
    dfnew1.show()
    
if __name__ == "__main__":
    test_codes()