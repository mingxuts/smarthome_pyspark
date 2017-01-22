'''
Created on 21 Jan 2017

@author: xuepeng
'''

import subprocess
import time


if __name__ == "__main__":
    
    status = subprocess.check_call(["/home/xuepeng/spark-2.1.0-bin-hadoop2.7/bin/spark-submit","--master","local[20]","demo-spark.py"])
    
    while status != 0:
        time.sleep(1)
        print "spark status is ",status
        
    print "spark status is ",status