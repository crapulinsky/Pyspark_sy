import os
from pyspark import SparkConf, SparkContext
'''
-------------------------------------------
    Description: TODO:
    SourceFile: ${NAME}
    Author: ${USER}
    Data: ${DATE}
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

    # spark context对象，简称sc
    conf = SparkConf().setMaster("local[*]").setAppName("{NAME}")  # 这里是名字
    sc = SparkContext(conf=conf)
    print(sc)
'''-------------------------------------------------'''










'''------------------------------------------------'''
sc.stop()