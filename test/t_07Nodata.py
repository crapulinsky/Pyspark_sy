from pyspark.sql import SparkSession
import os

if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("RDD with SparkSession") \
        .master("local") \
        .getOrCreate()

    sc = spark.sparkContext

    #定义rdd1
    rdd1 = sc.range(1,10,2)
    #打印rdd1
    print(rdd1)
    print('---------------------------------------------------') #打印封坟线
    #收集rdd1为rdd2
    rdd2 = rdd1.collect()
    #打印rdd2
    print(rdd2)

    spark.stop()
