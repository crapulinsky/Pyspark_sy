import os
import time
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType,StructField,DoubleType,StringType
from pyspark.ml.tuning import ParamGridBuilder,TrainValidationSplit
import pyspark.pandas as pd
'''
-------------------------------------------
    Description: 自动训练并评估最佳模型
    SourceFile: m2_tree
    Author: SMOG
    Data: 2024/12/26
-------------------------------------------
'''


if __name__ == '__main__':
    # 程序开始运行
    start_time = time.time()

    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    # 配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    # pandas环境变量
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

    # .config("spark.executor.resource.gpu.amount", "1") \
    # .config("spark.task.resource.gpu.amount", "1") \
    # .config("spark.executor.resource.gpu.memory.mb","4096") \
    # .config("spark.rapids.sql.enabled","true") \

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("m2_tree") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()




    # 加载数据
    print("加载数据：")
    data_without_header = spark.read.format("csv") \
        .option("header", "false") \
        .option("inferSchema", "true") \
        .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/covertype/covtype.data")

    # 增加列名
    colnames = ["Elevation", "Aspect", "Slope",
                "Horizontal_Distance_To_Hydrology",
                "Vertical_Distance_To_Hydrology", "Horizontal_Distance_To_Roadways",
                "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
                "Horizontal_Distance_To_Fire_Points"] + \
               [f"Wilderness_Area_{i}" for i in range(4)] + \
               [f"Soil_Type_{i}" for i in range(40)] + \
               ["Cover_Type"]

    data = data_without_header.toDF(*colnames).withColumn("Cover_Type", col("Cover_Type").cast(DoubleType()))
    data.show(5)

    # 随机切分数据集
    print("随机切分数据集：")
    (train_data, test_data) = data.randomSplit([0.9, 0.1])
    train_data.cache()
    test_data.cache()

    # 特征列
    input_cols = colnames[:-1]
    assembler = VectorAssembler(inputCols=input_cols, outputCol="featureVector")
    assembler_train_data = assembler.transform(train_data)

    # 构建模型
    classifier = DecisionTreeClassifier(seed=1234, labelCol="Cover_Type"
                                        , featuresCol="featureVector",
                                        predictionCol="prediction")
    pipeline = Pipeline(stages=[assembler, classifier])

    # 参数调优
    paramGrid = ParamGridBuilder() \
        .addGrid(classifier.impurity, ["gini", "entropy"]) \
        .addGrid(classifier.maxDepth, [1, 20]) \
        .addGrid(classifier.maxBins, [40, 300]) \
        .addGrid(classifier.minInfoGain, [0.0, 0.05]) \
        .build()

    # 评估器
    multiclassEval = MulticlassClassificationEvaluator() \
        .setLabelCol("Cover_Type") \
        .setPredictionCol("prediction") \
        .setMetricName("accuracy")

    # 训练和验证
    validator = TrainValidationSplit(seed=1234,
                                     estimator=pipeline,
                                     evaluator=multiclassEval,
                                     estimatorParamMaps=paramGrid,
                                     trainRatio=0.9)
    '''
    stages[0]是评估,stages[1]是分类
    '''
    # 训练模型
    print("开始训练：")
    validator_model = validator.fit(train_data)
    best_model = validator_model.bestModel

    # 重要特征
    print("重要特征：")
    dt_model = best_model.stages[1]
    print(pd.DataFrame(dt_model.featureImportances.toArray(),
                 index = input_cols,columns=['importance']) \
                .sort_values(by = "importance",ascending=False))

    # 失败代码，待修改
    # print("重要特征：")
    # dt_model = best_model.stages[1]
    # importances = dt_model.featureImportances
    # schema = StructType([
    #     StructField("Feature", StringType(), True),
    #     StructField("Importance", DoubleType(), True)
    # ])
    # importance_df = spark.createDataFrame(
    #     data= [(input_cols[i], importances[i]) for i in range(len(input_cols))],
    #     schema=schema
    # ).orderBy(col("Importance").desc())
    # importance_df.show()

    # 输出最佳模型的参数
    print("最佳模型参数：")
    pprint(best_model.stages[1].extractParamMap())

    # 评估最佳模型
    print("评估最佳模型：")
    predictions = best_model.transform(test_data)

    evaluator = MulticlassClassificationEvaluator(labelCol="Cover_Type",
                                                  predictionCol="prediction")
    accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    f1_score = evaluator.setMetricName("f1").evaluate(predictions)

    print(f"测试集准确率: {accuracy:.4f}")
    print(f"测试集F1分数: {f1_score:.4f}")

    # 预测
    print("预测：")
    predictions = best_model.transform(train_data)
    predictions.show()

    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    # print("jdk 11.0.20:")
    print("运行时间:", end_time - start_time)