'''
Created on 10 Nov 2015
@author: xuepeng
'''

from pyspark import SparkContext

def wordCount():
    logFile = "/home/xuepeng/spark-2.1.0-bin-hadoop2.7/README.md"  # Should be some file on your system
    sc = SparkContext("local", "Simple App")
    logData = sc.textFile(logFile)

    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
    

    
def testSC():
    sc = SparkContext("local", "Simple App")
    
    textFile = sc.textFile("/home/xuepeng/spark-2.1.0-bin-hadoop2.7/README.md")
    
    print textFile.count()

if __name__ == '__main__':
#     wordCount()
    testSC()
    