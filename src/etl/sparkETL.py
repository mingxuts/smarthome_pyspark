# -*- coding: UTF-8 -*-
#原始数据文件放在当前目录下的csv目录下，数据清洗最终结果会放在当前目录下的output目录下
#程序运行方式（命令行方式）：／path／spark-summit etlstart.py

import os, codecs
from pyspark import  SparkContext
import shutil
from datetime import datetime


#生成中间文件（类似于数据库中格式），并存储到results.txt文件中
def processfile(record):
    data = []
    line_list = record[1].split('\n')
    file_No = record[0].split('.')[0].split('/')[-1]#modifyed by Xueping
    user=province=city = ""
    xulie = 1               #modifyed by Xueping
    data_record = []
    last_data_record = []   #modifyed by Xueping
    first_data_record = []  #modifyed by Xueping
    count = 0
    

    for item in line_list:#for each user information
        if item == '':
            break
        item_list = item.split(",")#change user information to list
        check = item_list[0]
        if check == "/":#one device content
            if item_list[2] == "poweron":
                if count == 0:#first one record in new sequence
                    first_data_record = [user,str(file_No)+"-"+str(xulie),str(1),item_list[8],"",item_list[3],item_list[4],\
                                         item_list[5],item_list[6],item_list[7],province,city] #modifyed by Xueping
                    xulie += 1 #modifyed by Xueping
                count += 1
                last_data_record = [user,str(file_No)+"-"+str(xulie),str(count),item_list[8],item_list[3],item_list[4],\
                                    item_list[5],item_list[6],item_list[7],province,city] #modifyed by Xueping
            else:#poweroff #modifyed by Xueping
                if len(first_data_record) > 0:
                    data_record = first_data_record
                    data_record[2] = last_data_record[2]
                    data_record[4] = last_data_record[3]
                    data.append(data_record)
                    last_data_record = []
                    first_data_record = []
                count = 0
           
        else:# another device id
            count = 0 #modifyed by Xueping
            user = check[3:]
            if item_list[1] == "None":
                province=city=""
                continue
            address = item_list[1]
            if address[2:4] == u"黑龙" or address[2:4] == u"内蒙":
                province = address[2:5]
                city = address[5:]
            else:
                province = address[2:4]
                city = address[4:]

    with codecs.open('results.txt',"a+","utf-8") as f1:
        for item in data:
            string = ','.join(item).encode('utf8')
            print >> f1,string.decode('utf8')

#把列表转换为str类型
def map_list_string(record):
    for item in record:
        string = u','.join(item).encode('utf8')
    return string

#add number of sequence
def addNoOfSequenceToString(record):
    record[1].insert(0,record[0])
    string = u','.join(record[1]).encode('utf8')
    return string

def addNoToSequence(record):
    record[1].insert(0,record[0])
    return record[1]

#过滤序列长度小于2的项
def filterone(line):
    try:
        if int(line[2]) > 1: #modifyed by Xueping
            return True
        else:
            return False
    except Exception:
        print line
        
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
    haveWeatherInfo = False
    time_user = datetime.strptime(str(time),"%Y-%m-%d %H:%M")
    for index,item in enumerate(line):
        if len(item) == 8:
            haveWeatherInfo = True
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
    if haveWeatherInfo :
        return line[index_f][1:]
    else:
        return ["","","","","","",""]

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
    file_dir = "/home/xuepeng/Desktop/etl"
    file_weather = "./china_dec_weather.txt"
    sc = SparkContext("local[20]", "First Spark App")
    #original Data
    raw_data = sc.wholeTextFiles(file_dir)
    #weather information
    data_weather = sc.textFile(file_weather).map(lambda line: line.split(","))\
                     .map(lambda line:(line[0]+" "+line[1],line)).cache()
 
    if os.path.exists('./results.txt'):
        os.remove('./results.txt')
    if os.path.exists('./final_results.txt'):
        os.remove('./final_results.txt')
    if os.path.exists('./first_results.txt'):
        os.remove('./first_results.txt')
#     if os.path.exists('./output_weather'):#modifyed by Xueping
#         shutil.rmtree('./output_weather') #modifyed by Xueping
    raw_data.foreach(processfile)

    firstStepData = sc.textFile('./results.txt').\
                    map(lambda line: line.split(",")).\
                    filter(filterone).map(lambda line:(line[0],line)).\
                    groupByKey().mapValues(list).filter(lambda item:len(item[1]) > 3).\
                    map(lambda item: (str(len(item[1])),item[1])).\
                    flatMapValues(lambda item:item).map(addNoToSequence)
                                        
    with codecs.open('first_results.txt',"a+","utf-8") as f1:
        for item in firstStepData.collect():
            string = ','.join(item).encode('utf8')
            print >> f1,string.decode('utf8')
    
    secondStepData = firstStepData.map(lambda line:(line[-1]+" "+line[4][0:10],line)).union(data_weather)\
                     .groupByKey().mapValues(list).filter(filterline).map(lambda line:line[1])\
                     .flatMap(mapone).map(lambda line:(line[1]+" "+line[4],line)).sortByKey().map(lambda line:line[1])\
#                      .map(lambda line:u",".join(line).encode('utf8')).collect()\
#                      .saveAsTextFile("./output_weather")\
                    
    with codecs.open('final_results.txt',"w","utf-8") as f1:
        for item in secondStepData.collect():
            string = ','.join(item).encode('utf8')
            print >> f1,string.decode('utf8')


    sc.stop()