import os

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
'''
-------------------------------------------
    Description: TODO:
    SourceFile: _013chengfa
    Author: SMOG
    Data: 2024/12/2
-------------------------------------------
'''
if __name__ == '__main__':
    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    #配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("RandomProcess") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)
    x = 1
    step = 100
    values = []
    df = spark.createDataFrame([(x,)],["value"])

    for i in range (step):
        rp = df.withColumn("value",
                           F.when(F.rand()>0.5,F.col("value")*0.9)
                           .otherwise(F.col("value")*1.1)
                           )
        sv = df.collect()[0]['value']
        values.append(sv)

    print(values)

    plt.plot(range(step), values)
    plt.xlabel('步骤')
    plt.ylabel('值')
    plt.title('PySpark 模拟的随机过程轨迹')
    plt.show()


    spark.stop()