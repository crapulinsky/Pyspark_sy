import os
import time
from pyspark.sql import *
from pyspark.sql.types import StructField, StringType, IntegerType, StructType
from pyspark.sql.functions import *
'''
-------------------------------------------
    Description: TODO:
    SourceFile: _016toDF1
    Author: SMOG
    Data: 2024/12/8
-------------------------------------------
'''
if __name__ == '__main__':
    start = time.time()
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


    munualschema = StructType([
        StructField("some",StringType(),True),
        StructField("col",StringType(),True),
        StructField("names",IntegerType(),False)
    ])

    rows = Row("hello",None,1)
    df = spark.createDataFrame([rows],schema=munualschema)
    df.show()

    df2 = df.select(
        expr("some as xxx1"),
        col("some").alias("xxx2"),
        column("some")
    )

    df2.show()

    df3 = df.select(
        expr("some"),lit(1)
    )
    df3.show()

    df4 = df.withColumn("with",expr("some==names"))

    df4.show()

    spark.stop()
    end = time.time()
    print("运行时间:",end-start)