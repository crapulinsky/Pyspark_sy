import os
import time
from pyspark.sql import SparkSession

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_050rw
    Author: SMOG
    Data: 2024/12/21
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
        .appName("t_050rw") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    df = spark.read.format("csv") \
        .option("header","true") \
        .option("mode","FAILFAST") \
        .option("inferSchema","true") \
        .option("path","/path/to/.csv") \
        .load()

    df.write.format("csv") \
        .mode("overwrite") \
        .save("/path/to/.csv")
    parquetdf = spark.read.format("parquet") \
                    .load("/path/to/parquet")
    parquetdf.write.format("parquet") \
                    .mode("overwrite") \
                    .save("/path/to/parquet")


    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)