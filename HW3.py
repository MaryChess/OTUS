### HomeWork3:
#Введение в Spark + Гид по безопасному Бостону
#Цель: В этом задании предлагается собрать статистику по криминогенной обстановке в разных районах Бостона, используя Apache Spark.
#Подробную инструкцию по выполнению заданий смотрите в документе
#https://docs.google.com/document/d/1Ezkk_vtjP3imz_-Ij0aglcyiWSbujgJC0pLbdcP4c0I/edit?usp=sharing

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

    crimeFacts = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("boston_crimes/crime.csv")

    #1 crimes_total - общее количество преступлений в этом районе
    crimeFacts.groupBy('DISTRICT').count().select('DISTRICT', col('count')\
                                                  .alias('crimes_total')).show()


    #2 crimes_monthly - медиана числа преступлений в месяц в этом районе
    crimeFacts.groupBy('DISTRICT','MONTH','YEAR','DAY_OF_WEEK').count()\
        .select('DISTRICT','MONTH','YEAR', col('count').alias('n'))\
        .groupBy('DISTRICT','MONTH','YEAR')\
        .agg(expr('percentile_approx(n, 0.5)').alias('crimes_monthly')).show()


    # 3 frequent_crime_types - три самых частых crime_type за всю историю
    # наблюдений в этом районе, объединенных через запятую с одним пробелом “, ” ,
    # расположенных в порядке убывания частоты
    offenseCodes = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("boston_crimes/offense_codes.csv")


    split_col = split(offenseCodes['NAME'], '-')
    offenseCodes_mod = offenseCodes.withColumn('frequent_crime_types', split_col.getItem(0))
    join_df = crimeFacts.join(broadcast(offenseCodes_mod),
                              offenseCodes.CODE == crimeFacts.OFFENSE_CODE)\
        .groupBy("DISTRICT","frequent_crime_types").count()


    windowSpec = Window.partitionBy("DISTRICT") \
        .orderBy(col("count").desc())

    join_df.select('*', rank().over(windowSpec).alias('rank'))\
    	.filter(col('rank') <= 3).select("DISTRICT","frequent_crime_types") \
    	.groupby("DISTRICT").agg(concat_ws(", ", collect_list(join_df.frequent_crime_types)))\
    	.show()


    #4 lat - широта координаты района, расчитанная как среднее по всем широтам инцидентов
    #5 ng - долгота координаты района, расчитанная как среднее по всем долготам инцидентов
    crimeFacts.groupBy("DISTRICT").agg(mean(crimeFacts.Lat).alias("Lat_m"),
                                       mean(crimeFacts.Long).alias("Long_m"))\
    	.select("DISTRICT", "Lat_m", "Long_m").distinct().show()
