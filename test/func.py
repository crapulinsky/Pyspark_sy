from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType

def pivot_summary(desc):
    desc_p = desc.toPandas()
        # transpose
    desc_p = desc_p.set_index('summary').transpose().reset_index()
    desc_p = desc_p.rename(columns={'index': 'field'})
    desc_p = desc_p.rename_axis(None, axis=1)
        # convert to Spark dataframe
    descT = DataFrame.createDataFrame(desc_p)
        # convert metric columns to double from string
    for c in descT.columns:
        if c == 'field':
            continue
        else:
            descT = descT.withColumn(c, descT[c].cast(DoubleType()))
    return descT

def crossTabs(scored: DataFrame, t: DoubleType) -> DataFrame:
  return  scored.selectExpr(f"score >= {t} as above", "is_match").\
           groupBy("above").pivot("is_match", ("true", "false")).\
          count()

