'''
Created on 17 Jan 2017

@author: Han Zheng
'''
import os
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import shutil

from smarthome.sparkETL import filterone


def addNoToSequence(record):
    record[1].insert(0,record[0])
    return record[1]

if __name__ == "__main__":
    
    if os.path.exists('./output'):#modifyed by Xueping
        shutil.rmtree('./output') #modifyed by Xueping
    
    sc = SparkContext("local[20]", "First Spark App")
    
    sqlContext = SQLContext(sc)
    
     
    weather_dir = "/home/xuepeng/Desktop/china_dec_weather.txt"
     
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true',charset="utf-8").load(weather_dir)
    rows = df.select("city")
    
    firstStepData = sc.textFile('./results.txt').\
        map(lambda line: line.split(",")).\
        filter(filterone).map(lambda line:(line[0],line)).\
        groupByKey().mapValues(list).filter(lambda item:len(item[1]) > 3).\
        map(lambda item: (str(len(item[1])),item[1])).\
        flatMapValues(lambda item:item).map(addNoToSequence).toDF()
        
    firstStepData.select("_1").show()

    sc.stop()