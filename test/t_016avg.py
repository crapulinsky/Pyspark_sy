import os
from pyspark.sql import SparkSession

'''
-------------------------------------------
    Description: TODO:
    SourceFile: _016avg
    Author: SMOG
    Data: 2024/12/6
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
    sc = spark.sparkContext

    # 创建 RDD
    rdd = sc.parallelize([("key", 2), ("word", 3), ("value", 4), ("key", 5), ("word", 6), ("key", 7)])


    rdd1 = rdd.combineByKey(lambda x :(x,1),
                            lambda acc,x :(acc[0]+x,acc[1]+1),
                            lambda acc1,acc2 :(acc1[0]+acc2[0],acc1[1]+acc2[1])
    )
    rdd2 = rdd1.map(lambda kv:(kv[0],float(kv[1][0]/kv[1][1])))
    rdd3 = rdd1.map(lambda kv:(kv[0],("avg",round(kv[1][0]/kv[1][1],2))))
    
    # 收集并打印结果
    print(rdd1.collect())
    print(rdd2.collect())
    print(rdd3.collect())
    # 关闭 SparkContext
    sc.stop()

    spark.stop()
