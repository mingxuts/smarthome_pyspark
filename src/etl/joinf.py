'''
Created on 18 Jan 2017

@author: Han Zheng
'''

# -*- coding: UTF-8 -*-

from pyspark import  SparkContext
from datetime import datetime
import shutil,os


#filter invaild line
def filterline(line):
    for item in line[1]:
        if len(item) == 8:
            continue
        return True
    return False

#find the right weather information
def fine_weather(line,time):
    time_delta = 0
    index_f = 0
    time_user = datetime.strptime(str(time),"%Y-%m-%d %H:%M")
    for index,item in enumerate(line):
        if len(item) == 8:
            time_weather_str = str(item[1]+" "+item[2])
            time_delta_tmp = 0
            time_weather = datetime.strptime(time_weather_str,"%Y-%m-%d %H:%M:%S")
            if time_weather > time_user:
                time_delta_tmp = (time_weather-time_user).seconds
            else:
                time_delta_tmp = (time_user-time_weather).seconds
            if time_delta == 0 and index_f == 0:
                time_delta = time_delta_tmp
                index_f = index
            if time_delta_tmp < time_delta:
                index_f = index
                time_delta = time_delta_tmp
    return line[index_f][1:]

#add weather information
def mapone(line):
    line_list = []
    for item in line:
        if len(item) != 8:
            weather = fine_weather(line,item[4])
            item.extend(weather)
            line_list.append(item)
    return line_list


if __name__ == "__main__":
    file_weather = "./china_dec_weather.txt"
    file_input = "input.txt"

    sc = SparkContext("local[8]", "Etlstart Spark App")

    if os.path.exists('./output_weather'):#modifyed by Xueping
        shutil.rmtree('./output_weather') #modifyed by Xueping

    data_weather = sc.textFile(file_weather).map(lambda line: line.split(",")).\
    map(lambda line:(line[0]+" "+line[1],line)).cache()

    data_tem = sc.textFile(file_input).map(lambda line:line.split(","))\
                 .map(lambda line:(line[-1]+" "+line[4][0:10],line)).union(data_weather)\
                 .groupByKey().mapValues(list).filter(filterline).map(lambda line:line[1])\
                 .flatMap(mapone).map(lambda line:u",".join(line).encode('utf8'))\
#     .saveAsTextFile("./output_weather")

    sc.stop()

