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


def runAlgorithm(algorithm,parameter):
    
    status = subprocess.check_call(["/home/xuepeng/spark-2.1.0-bin-hadoop2.7/bin/spark-submit","--master","local[20]",algorithm])    
    return status

def getJobs():
    
    jobFinished = False
    
    jobURL = 'http://localhost:8180/api/get_jobs'
    
    myResponse = requests.get(jobURL)
    if(myResponse.ok):
        
        if myResponse.content != "":
            
            jData = json.loads(myResponse.content)
            pre_jobid = jData["id"]
            pre_algName = jData["algorithm"]
            pre_parameters = jData["parameter"]
            runJob(pre_jobid, pre_algName, pre_parameters)
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
                
                if jobFinished:
                    jobFinished = False
                    runJob(jobid, algName, parameters)
        else:
            myResponse.raise_for_status()
            
        jobFinished = percentage()[1]   
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
    statusURL = "http://localhost:8180/api/job_status"
    job_result = "http://localhost:8180/api/job_result"
    
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
            print "Job {0} is finished!".format(jobId)
            
            break
        
        time.sleep(3)         

def runJob(jobid,algorithm,parameter):
    
    algThread = Thread(target=runAlgorithm, args=("demo-spark.py",parameter))
    algThread.start()
    
    time.sleep(6)
    
    percThread = Thread(target=loopPercentage, args=(jobid,algorithm))
    percThread.start()


if __name__ == "__main__":
    
    getJobs()
    