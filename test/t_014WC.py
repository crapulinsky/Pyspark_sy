import os
import time
from pyspark.sql import SparkSession
'''
-------------------------------------------
    Description: TODO:
    SourceFile: _014WC
    Author: SMOG
    Data: 2024/12/6
-------------------------------------------
'''
if __name__ == '__main__':
    #程序开始运行
    start_time = time.time()
    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    #配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # 创建 SparkSession
    spark = SparkSession.builder \
                        .appName("WC") \
                        .master("local[3]") \
                        .getOrCreate()
    print(spark)
    sc = spark.sparkContext

    rdd1 = sc.textFile("file:///D:\XXX\PycharmProjects\Pyspark_sy\datas\WC\data.txt")
    rdd2 = rdd1.filter(lambda x :len(x) > 0) \
            .flatMap(lambda line: line.split(" ") ) \
            .map(lambda x :(x,1)) \
            .combineByKey(lambda x : x,
                          lambda agg1,x: agg1 + x,
                          lambda agg2,agg1: agg2 + agg1)

    print(rdd2)
    result = rdd2.take(10)
    print(result)
    rdd2.saveAsTextFile("file:///D:/XXX/PycharmProjects/Pyspark_sy/datas/result")

    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)