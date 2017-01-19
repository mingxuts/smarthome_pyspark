'''
Created on 19 Jan 2017

@author: xuepeng
'''
import codecs
import os
from pyspark.context import SparkContext
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils


speed = {"auto":0,"silence":1,"low":2,"mid":3,"high":4,"super":5}
direction = {"auto":0,"vdir":1,"hdir":2}
mode = {"wind":0,"cool":1,"heat":2,"auto":3,"dehu":4}


def dataPrepareStepOne(weatherList):
    
    sortList = sorted(weatherList,key = lambda item:item[2]) 
    rowList = []
    length = len(sortList)
    for x in range(0,length-3):
        row = []
        #    airTemp
        row.append(sortList[x][10])
        row.append(sortList[x+1][10])
        row.append(sortList[x+2][10])
        #air Speed
        row.append(sortList[x][6])
        row.append(sortList[x+1][6])
        row.append(sortList[x+2][6])
        #air direction
        row.append(sortList[x][7])
        row.append(sortList[x+1][7])
        row.append(sortList[x+2][7])
        
        #air mode
        row.append(sortList[x][8])
        row.append(sortList[x+1][8])
        row.append(sortList[x+2][8])
        
        #predict parameters
        row.append(sortList[x+3][15])
        row.append(sortList[x+3][6])
        row.append(sortList[x+3][7])
        row.append(sortList[x+3][8])
        row.append(sortList[x+3][10])
        rowList.append(row)
    return rowList

def filterTemp(item):
    if int(item[0]) >23 and int(item[0]) <29 and \
        int(item[1]) >23 and int(item[1]) <29  and \
        int(item[2]) >23 and int(item[2]) <29 and \
        int(item[-1]) >23 and int(item[-1]) <29 \
        and item[12]!= "":
        return True
    else :
        return False
    
def transformer(item):
    item[0] = str(int(item[0])-24)
    item[1] = str(int(item[1])-24)
    item[2] = str(int(item[2])-24)
    item[3] = str(speed[item[3]])
    item[4] = str(speed[item[4]])
    item[5] = str(speed[item[5]])
    item[6] = str(direction[item[6]])
    item[7] = str(direction[item[7]])
    item[8] = str(direction[item[8]])
    item[9] = str(mode[item[9]])
    item[10] = str(mode[item[10]])
    item[11] = str(mode[item[11]])
  
    item[13] = str(speed[item[13]])
    item[14] = str(direction[item[14]])
    item[15] = str(mode[item[15]])
    item[16] = str(int(item[16])-24)
    return item

def labelCombination(s):
        
        classLabel = int(s[-1])*90+int(s[-2])*1+int(s[-3])*5+int(s[-4])*15
       
        s.pop()
        s.pop()
        s.pop()
        s.pop()
        
        s.append(str(classLabel))
        
        return s
    
def myFunc(words):
#     words = s.split(",")
    return words[-1]+' 1:'+words[0]+' 2:'+words[1]+' 3:'+words[2]+' 4:'+words[3] +' 5:'+words[4]+' 6:'+words[5]+' 7:'+words[6]+' 8:'+words[7] +' 9:'+words[8]+' 10:'+words[9]+' 11:'+words[10]+' 12:'+words[11]+' 13:'+words[12]
  

if __name__ == "__main__":
    
#     if os.path.exists('results/steponedata_results.txt'):
#         os.remove('results/steponedata_results.txt')
    if os.path.exists('results/steponedataLimitedTemp_results.txt'):
        os.remove('results/steponedataLimitedTemp_results.txt')
        
    sc = SparkContext("local[20]", "PythonRandomForestClassificationExample")
    distFile = sc.textFile("results/final_results.txt")
    stepOneData = distFile.map(lambda item : item.split(",")).map(lambda item :(item[1],item))\
                    .groupByKey().mapValues(list).map(lambda item:item[1])\
                    .flatMap(dataPrepareStepOne)
                    
    stepOneDataLimitedTemp = stepOneData.filter(filterTemp).map(transformer).map(labelCombination).map(myFunc)
            
#     with codecs.open('results/steponedata_results.txt',"a+","utf-8") as f1:
#         for item in stepOneData.collect():
#             string = ','.join(item).encode('utf8')
#             print >> f1,string.decode('utf8')
#             
    with codecs.open('results/steponedataLimitedTemp_results.txt',"w","utf-8") as f1:
        for item in stepOneDataLimitedTemp.collect():
#             string = ','.join(item).encode('utf8')
            print >> f1,item.decode('utf8')
    
    data = MLUtils.loadLibSVMFile(sc, 'results/steponedataLimitedTemp_results.txt')
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    #  Note: Use larger numTrees in practice.
    #  Setting featureSubsetStrategy="auto" lets the algorithm choose.
    model = RandomForest.trainClassifier(trainingData, numClasses=448, categoricalFeaturesInfo={},
                                         numTrees=5, featureSubsetStrategy="auto",
                                         impurity='gini', maxDepth=10, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(
        lambda lp: lp[0] != lp[1]).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification forest model:')
       
#     for item in distFile:
#         savedFile.write("%s\n" % item)        
