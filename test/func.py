from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType

# 快速转秩
def pivot_summary(desc):
    desc_p = desc.toPandas()

    desc_p = desc_p.set_index('summary').transpose().reset_index()
    desc_p = desc_p.rename(columns={'index': 'field'})
    desc_p = desc_p.rename_axis(None, axis=1)

    descT = DataFrame.createDataFrame(desc_p)

    for c in descT.columns:
        if c == 'field':
            continue
        else:
            descT = descT.withColumn(c, descT[c].cast(DoubleType()))
    return descT
# 成绩区分
def crossTabs(scored: DataFrame, t: DoubleType) -> DataFrame:
  return  scored.selectExpr(f"score >= {t} as above", "is_match").\
           groupBy("above").pivot("is_match", ("true", "false")).\
          count()

# 数据探索与统计
def explore_data(data):
    print("数据概览：")
    data.printSchema()
    print("数据统计描述：")
    data.describe().show()

# 保存模型的重要特征
def save_feature_importances(model, input_cols, file_path):
    import pandas as pd
    feature_importances = pd.DataFrame(model.featureImportances.toArray(),
                                       index=input_cols, columns=['importance']) \
                             .sort_values(by="importance", ascending=False)
    feature_importances.to_csv(file_path, index=True)
    print(f"重要特征已保存到 {file_path}")


