import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

'''
-------------------------------------------
    Description: TODO:
    SourceFile: _04ppp
    Author: SMOG
    Data: 2024/11/24
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

    spark = SparkSession.builder \
        .appName("SQL") \
        .getOrCreate()

    table1 = spark.createDataFrame([(1, "a"), (2, "b")], ["a", "key"])
    table2 = spark.createDataFrame([(3, "a"), (4, "b"), (5, "b")], ["b", "key"])

    table1.createOrReplaceTempView("table1")
    table2.createOrReplaceTempView("table2")
    '''-----------------------------------------------'''
    result = spark.sql('''
      SELECT m1.a, m2.b
      FROM table1 m1
      INNER JOIN table2 m2
      ON m1.key = m2.key
      ORDER BY m1.a, m2.b
    ''')
    '''-----------------------------------------------'''
    result.show()

    spark.stop()

