# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
#
# spark = SparkSession.builder.appName("LoadData").getOrCreate()
# df = spark.read.csv("file:///D:/XXX/PycharmProjects/Pyspark_sy/datas/WCWC/data.csv", header=False, inferSchema=True)
#
# #定义df
# df = df.toDF("X", "Y")
#
#
# # 创建新的列 X_squared，它是 X 的平方
# df_with_square = df.withColumn("X_squared", col("X") ** 2)
# #select y
# df_Y = df.select("y")
# #count计数
# df_count = df.count()
#
# # 查看结果
# df_with_square.show()
# #打印Y列
# df_Y.show()
# #打印count计数
# print(f'总共有{df_count}条数据')

from pyspark import SparkContext
import os
if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    # 初始化 SparkContext
    sc = SparkContext("", "Map Example")

    # 创建 RDD
    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data)

    # 使用 map 操作将每个元素平方
    squared_rdd = rdd.map(lambda x: x ** 2)

    # 收集并打印结果
    print(squared_rdd.collect())

    # 关闭 SparkContext
    sc.stop()

