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
    conf = SparkConf().setMaster("local[*]").setAppName("平均值")
    sc = SparkContext(conf=conf)

    print(sc)
    rdd = sc.parallelize([('a', 3), ('b', 5), ('a', 7), ('b', 2)])

    result = rdd.combineByKey(
        (lambda value: (value, 1)),  # 初始值: (值, 计数)
        (lambda acc, value: (acc[0] + value, acc[1] + 1)),  # 在每个分区内合并
        (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))  # 跨分区合并
    )

    # 计算平均值
    result = result.mapValues(lambda x: x[0] / x[1])

    print(result.collect())

    rdd = sc.parallelize([("a", "apple banana"), ("b", "cherry date"), ("c", "fig grape")])
    result = rdd.flatMap(lambda x: [(x[0], word) for word in x[1].split()])
    print(result.collect())


