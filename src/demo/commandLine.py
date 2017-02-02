'''
Created on 21 Jan 2017

@author: xuepeng
'''

import argparse

from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql import SparkSession




def oneHot():
    
    spark = SparkSession \
        .builder \
        .appName("OneHotEncoderExample") \
        .getOrCreate()
    
    df = spark.createDataFrame([
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "b"),
    (6, "c")
    ], ["id", "category"])
    
    stringIndexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
    model = stringIndexer.fit(df)
    indexed = model.transform(df)
    
    encoder = OneHotEncoder(inputCol="categoryIndex", outputCol="categoryVec")
    encoded = encoder.transform(indexed)
    encoded.show()
    
def temporaryFile():
    import tempfile


    temp = tempfile.NamedTemporaryFile()
    try:
        temp.write('Some data')
        temp.seek(0)
        
        print temp.read()
        print 'temp:', temp
        print 'temp.name:', temp.name
    finally:
        # Automatically cleans up the file
        temp.close()



if __name__ == "__main__":
    
#     parser = argparse.ArgumentParser(description='Process some integers.')
#     parser.add_argument('integers', metavar='N', type=int, nargs='+',
#                         help='an integer for the accumulator')
#     parser.add_argument('--sum', dest='accumulate', action='store_const',
#                         const=sum, default=max,
#                         help='sum the integers (default: find the max)')
#     
#     args = parser.parse_args()
#     print args.accumulate(args.integers)

#     oneHot()
    temporaryFile()