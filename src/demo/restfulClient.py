'''
Created on 21 Jan 2017

@author: xuepeng
'''

from base64 import b64encode
import codecs
import json
import subprocess
from threading import Thread
import time

import requests
from requests.exceptions import ConnectionError

global jobFinished 


def runAlgorithm(algorithm,parameter,filename):
    
    if algorithm.lower() == "randomforest":
        algorithm = "RandomForest"
        ps = parameter.split(" ")
#         numClasses  = ps[0].split("=")[1]
        impurity  = ps[1].split("=")[1]
        maxDepth  = ps[2].split("=")[1]
        maxBins  = ps[3].split("=")[1]
        minInstancesPerNode  = ps[4].split("=")[1]
        minInfoGain  = ps[5].split("=")[1]
        numTrees=ps[6].split("=")[1]
        featureSubsetStrategy=ps[7].split("=")[1]
        subprocess.check_call(["/home/xuepeng/spark-2.1.0-bin-hadoop2.7/bin/spark-submit","--master","local[20]","Models_spark.py", \
                              algorithm, filename, impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain,numTrees,featureSubsetStrategy])
    elif algorithm.lower() == "decisiontree":
        algorithm = "DecisionTree"
        ps = parameter.split(" ")
        
#         numClasses  = ps[0].split("=")[1]
        impurity  = ps[1].split("=")[1]
        maxDepth  = ps[2].split("=")[1]
        maxBins  = ps[3].split("=")[1]
        minInstancesPerNode  = ps[4].split("=")[1]
        minInfoGain  = ps[5].split("=")[1]
        subprocess.check_call(["/home/xuepeng/spark-2.1.0-bin-hadoop2.7/bin/spark-submit","--master","local[20]","Models_spark.py", \
                               algorithm, filename, impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain])    
    

def deleteJobs():
    
    jobFinished = False
    
    jobURL = 'http://115.146.87.170/api/get_jobs'
    
    while True:
        
        myResponse = requests.get(jobURL)
           
        if(myResponse.ok):
            if myResponse.content != "":
                print myResponse.content
                jData = json.loads(myResponse.content)
                jobid = jData["id"]
                algName = jData["algorithm"]
                
                jobFinished = False
                #Send status
                statusJson = {'jobid': jobid, 'percentage': 0,"display_name":algName}
                requests.post("http://115.146.87.170/api/job_status", json=statusJson)
                print str(jobid) + "  "+ algName +", no parameters!"
                
                #send result
                text = "Should provide parameters! The job is killed!"
                resultJson = {'jobid': jobid, "result":b64encode(text)}
                requests.post("http://115.146.87.170/api/job_result", json=resultJson)
                #set job status
                jobFinished = True
        else:
            myResponse.raise_for_status()
            
        jobFinished = percentage()[1]   
        time.sleep(3)

def getJobs():
    
    jobFinished = False
    
    jobURL = 'http://115.146.87.170/api/get_jobs'
    
    myResponse = requests.get(jobURL)
    if(myResponse.ok):
        
        if myResponse.content != "":
            
            jData = json.loads(myResponse.content)
            pre_jobid = jData["id"]
            pre_algName = jData["algorithm"]
            pre_parameters = jData["parameter"]
            pre_fileName = jData["filename"]
            runJob(pre_jobid, pre_algName, pre_parameters,pre_fileName)
        else:
            jobFinished = True
    else:
        myResponse.raise_for_status()
    
    while True:
        
        myResponse = requests.get(jobURL)
           
        if(myResponse.ok):
            if myResponse.content != "":
                print myResponse.content
                jData = json.loads(myResponse.content)
                jobid = jData["id"]
                algName = jData["algorithm"]
                parameters = jData["parameter"]
                fileName = jData["filename"]
                
                if parameters == "": #No parameters
                    jobFinished = False
                    #Send status
                    statusJson = {'jobid': jobid, 'percentage': 0,"display_name":algName}
                    requests.post("http://115.146.87.170/api/job_status", json=statusJson)
                    print str(jobid) + "  "+ algName +", no parameters!"
                    
                    #send result
                    text = "Should provide parameters! The job is killed!"
                    resultJson = {'jobid': jobid, "result":b64encode(text)}
                    requests.post("http://115.146.87.170/api/job_result", json=resultJson)
                    #set job status
                    jobFinished = True
                else:
                    if jobFinished:
                        jobFinished = False
                        runJob(jobid, algName, parameters,fileName)
        else:
            myResponse.raise_for_status()
            
        jobFinished = percentage()[1]   
        
        myResponse.close()
        time.sleep(3)
        
    
        
def percentage():
    
    stageURL = ""
    percentage = 0.0
    
    try:   
        r1 = requests.get('http://localhost:4040/api/v1/applications/')
        if(r1.ok):
         
            jData = json.loads(r1.content)
#             print jData[0]["id"]
            stageURL = "http://localhost:4040/api/v1/applications/"+jData[0]["id"] + "/stages"
            
        else:
            r1.raise_for_status()
            
        r2 = requests.get(stageURL)
        if(r2.ok):
         
            jData = json.loads(r2.content)
            stageNum = len(jData)
            completeNum = 0.0
            for stage in jData:
                if stage["status"] == "COMPLETE" : completeNum =  completeNum + 1.0
            
            if stageNum == 0:
                percentage = 0.0
            else:            
                percentage = float(completeNum/stageNum)*100
         
            
        else:
            r1.raise_for_status()
            percentage = 0.0
        
        return (percentage,False)
    
    except ConnectionError:
        return (100,True)


def loopPercentage(jobId,displayName):
    
    previousPercentage = 0.0
    statusURL = "http://115.146.87.170/api/job_status"
    job_result = "http://115.146.87.170/api/job_result"
    job_file = "http://115.146.87.170/api/upload_c_file"
        
    while True:
        
        currentPercentage = percentage()[0]
        status = percentage()[1]
        
        if currentPercentage >= previousPercentage:
            
            previousPercentage = currentPercentage
            statusJson = {'jobid': jobId, 'percentage': previousPercentage,"display_name":displayName}
            requests.post(statusURL, json=statusJson)
            
        if status:
            f1 = codecs.open('results.txt',"r","utf-8")
            text = f1.read()
            
            resultJson = {'jobid': jobId, "result":b64encode(text)}
            requests.post(job_result, json=resultJson)
            
#             if displayName.lower() == "decisiontree":
                
            files = {'uploadedFile': codecs.open('output/structure_rule.txt',"r","utf-8")}
            resultFile = {'jobid': jobId}
            response = requests.post(job_file, data=resultFile,files=files)
            print response.text
            print "Job {0} is finished!".format(jobId)
            
            break
        
        time.sleep(3)         

def runJob(jobid,algorithm,parameter,filename):
    
    algThread = Thread(target=runAlgorithm, args=(algorithm,parameter,filename))
    algThread.start()
    
    time.sleep(6)
    
    percThread = Thread(target=loopPercentage, args=(jobid,algorithm))
    percThread.start()


if __name__ == "__main__":
    
    getJobs()
#     deleteJobs()

    