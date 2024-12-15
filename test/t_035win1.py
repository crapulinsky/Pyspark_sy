import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, max, dense_rank, row_number,rank
from pyspark.sql.window import Window

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_035win1
    Author: SMOG
    Data: 2024/12/14
-------------------------------------------
'''
if __name__ == '__main__':
    # 程序开始运行
    start_time = time.time()
    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    # 配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("t_035win1") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/retail-data/by-day/2010-12-01.csv") \
        .coalesce(5)

    windows = Window.partitionBy("CustomerID") \
                .orderBy("Quantity") \
                .rowsBetween(Window.unboundedPreceding,Window.currentRow)
    dense_rank = dense_rank().over(windows)
    row_number = row_number().over(windows)
    rank = rank().over(windows)

    df.select(col("InvoiceNo"),col("StockCode"),col("Quantity"),rank.alias("rank"),dense_rank.alias("dense_rank"),row_number.alias("row_number")).show()

    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)
