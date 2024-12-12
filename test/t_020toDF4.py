import os
from pyspark.sql import SparkSession


'''
-------------------------------------------
    Description: TODO:
    SourceFile: _020toDF4
    Author: SMOG
    Data: 2024/12/8
-------------------------------------------
'''
if __name__ == '__main__':
    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    # 配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("{NAME}") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    df = spark.read.format("csv") \
        .option("header","true") \
        .option("inferSchema","true") \
        .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/retail-data/by-day/2010-12-01.csv")
    df.printSchema()

    #近似分位数
    result = df.stat.approxQuantile("UnitPrice",[0.25,0.5,0.75],0.05)
    print(result)

    spark.stop()
