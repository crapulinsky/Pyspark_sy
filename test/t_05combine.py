import os
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':


    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    #配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    #spark context对象，简称sc
    conf = SparkConf().setMaster("local[*]").setAppName("WC") #数单词
    sc = SparkContext(conf=conf)
    print(sc)

    #Rdd运算

    fileRdd = sc.textFile("file:///D:/XXX/PycharmProjects/Pyspark_sy/datas/WC/data.txt")
    '''
    print(fileRdd.getNumPartitions())
    rsRdd = fileRdd.filter(lambda x:len(x) > 0) \
        .flatMap(lambda line:line.strip().split()) \
        .map(lambda word:(word,1)) \
        .combineByKey(
        lambda value: value,              # 创建初始值，每个键的第一个值直接使用
        lambda acc, value: acc + value,  # 分区内累加
        lambda acc1, acc2: acc1 + acc2   # 分区间累加
    )
    '''

    rsRdd = fileRdd.filter(lambda x: len(x) > 0).flatMap(lambda line: line.strip().split()) \
            .map(lambda word: (word,1)) \
            .combineByKey(
            lambda value: value,
            lambda acc,vlue: acc + vlue,
            lambda acc1,acc2: acc1 + acc2
    )
    # rsRdd.saveAsTextFile("file:///D:/XXX/PycharmProjects/Pyspark_sy/datas/WCWC/result1")
    print(rsRdd.collect())  # 输出合并后的结果
    #print(result.collect())

    sc.stop()