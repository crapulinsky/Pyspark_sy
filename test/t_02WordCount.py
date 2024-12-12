import os

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':


    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # jdk
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # Spark
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # Hadoop
    # python解释器
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # spark context
    conf = SparkConf().setMaster("local[*]").setAppName("WC") #数单词
    sc = SparkContext(conf=conf)
    print(sc)
    # Rdd运算
    fileRdd = sc.textFile("file:///D:/XXX/PycharmProjects/Pyspark_sy/datas/WC/data.txt")
    rsRdd = (fileRdd.filter(lambda x:len(x) > 0)
             .flatMap(lambda line:line.strip().split())
             .map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b))
    #rsRdd.saveAsTextFile("file:///D:/XXX/PycharmProjects/Pyspark_sy/datas/WC/result1") #保存数据用的，我注释掉了

    # 显示计算结果
    print(rsRdd.collect())

    sc.stop()