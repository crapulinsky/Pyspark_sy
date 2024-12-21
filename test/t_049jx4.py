import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,rank,row_number
'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_049jx4
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
        .appName("t_049jx4") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    data = [
        ("Alice", "2024-01-01", 90),
        ("Bob", "2024-01-01", 85),
        ("Alice", "2024-01-02", 92),
        ("Bob", "2024-01-02", 88),
        ("Cathy", "2024-01-01", 95),
        ("Cathy", "2024-01-02", 91),
    ]

    schema = ["Name", "Date", "Score"]
    df = spark.createDataFrame(data, schema)

    window_spec = Window.orderBy(col("Score").desc())

    rankDf = df.withColumn("Rank", rank().over(window_spec))

    rankDf.show()



    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)
