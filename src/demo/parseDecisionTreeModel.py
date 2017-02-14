'''
Created on 2 Feb 2017

@author: xuepeng
'''

import codecs
import json
import os
from pyspark import SparkContext
from pyspark.mllib.tree import DecisionTreeModel


# Parser
def parse(lines):
    block = []
    while lines :
        
        if lines[0].startswith('If'):
            bl = ' '.join(lines.pop(0).split()[1:]).replace('(', '').replace(')', '')
            block.append({'name':bl, 'children':parse(lines)})
            
            
            if lines[0].startswith('Else'):
                be = ' '.join(lines.pop(0).split()[1:]).replace('(', '').replace(')', '')
                block.append({'name':be, 'children':parse(lines)})
        elif not lines[0].startswith(('If','Else')):
            block2 = lines.pop(0)
            block.append({'name':block2})
        else:
            break    
    return block

global dicts

# Parser
def parseToC(lines):
    block = ''
    while lines :
        
        if lines[0].startswith('If'):
            line = lines.pop(0)
            
            if 'not in' in line:
                line = line.replace('(', '').replace(')', '')
                ln = line.split()[1:]
                bl = 'if(strstr(\"' + ln[4].replace('{','').replace('}','')+ '\",' +ln[0]+ '['+ln[1]+'])==NULL)'
            elif 'in' in line:
                line = line.replace('(', '').replace(')', '')
                ln = line.split()[1:]
                bl = 'if(strstr(\"' + ln[3].replace('{','').replace('}','')+ '\",' +ln[0]+ '['+ln[1]+'])!=NULL)'
            else:
                ln = line.split()[1:]
                bl = 'if(atof' + ln[0]+ '['+ln[1]+']) ' + ' '.join(ln[2:])
            
            block = block + bl + '{\n' + parseToC(lines) + '}'
            
            
            if lines[0].startswith('Else'):
                
                line = lines.pop(0)
            
                if 'not in' in line:
                    line = line.replace('(', '').replace(')', '')
                    ln = line.split()[1:]
                    bl = 'if(strstr(\"' + ln[4].replace('{','').replace('}','')+ '\",' +ln[0]+ '['+ln[1]+'])==NULL)'
                elif 'in' in line:
                    line = line.replace('(', '').replace(')', '')
                    ln = line.split()[1:]
                    bl = 'if(strstr(\"' + ln[3].replace('{','').replace('}','')+ '\",' +ln[0]+ '['+ln[1]+'])!=NULL)'
                else:
                    ln = line.split()[1:]
                    bl = 'else if(atof' + ln[0]+ '['+ln[1]+']) ' + ' '.join(ln[2:])
                
                block = block + bl + '{\n' + parseToC(lines) + '}'
        elif not lines[0].startswith(('If','Else')):
            block2 = lines.pop(0).split()[1]
            block = block + 'return ' + block2 +';'
        else:
            break    
    return block

# Convert Tree to C scripts
def tree_C(tree,resultsFile):
    
    if os.path.exists(resultsFile):
        os.remove(resultsFile)
    if os.path.exists('output/decisionTree.c'):
        os.remove('output/decisionTree.c')
    if os.path.exists('output/main.c'):
        os.remove('output/main.c')
    
    data = []
    f1  = codecs.open(resultsFile,"a+","utf-8")
    for line in tree.splitlines() : 
        if line.strip():
            print >> f1,line.decode('utf8')
            line = line.strip()
            data.append(line)
        else : break
        if not line : break
    cStr  = parseToC(data[1:])
    
    f2  = codecs.open('output/decisionTree.c',"a+","utf-8") 
    print >> f2,"#include <string.h>".decode('utf8')
    print >> f2,"#include <stdlib.h>".decode('utf8')
    print >> f2,"float decisionTree(char feature[13][4]){".decode('utf8')
    print >> f2,cStr.decode('utf8')
    print >> f2,"}".decode('utf8')
    
    mainFuction  = codecs.open('output/main.c',"a+","utf-8") 
    print >> mainFuction,"#include <string.h>".decode('utf8')
    print >> mainFuction,"#include <stdlib.h>".decode('utf8')
    print >> mainFuction,"#include <stdio.h>".decode('utf8')
    print >> mainFuction,"float decisionTree(char feature[13][4]);".decode('utf8')
    print >> mainFuction,"int main(int argc, char * argv[]){".decode('utf8')
    print >> mainFuction,"//feature discription:feature[0]-[2] are 3 latest temperatures, feature[3]-[5] are 3 latest wind speed,".decode('utf8')
    print >> mainFuction,"//feature[6]-[8] are 3 latest wind direction,feature[9]-[11] are 3 latest mode,feature[12] is forecast temperature.".decode('utf8')
    print >> mainFuction,"//the item value is the index of following map:".decode('utf8')
    print >> mainFuction,"//The temperature range is between 24 and 28,so temperature={24:0,25:1,26:2,27:3,28:4}".decode('utf8')
    print >> mainFuction,"""//speed = {"auto":0,"silence":1,"low":2,"mid":3,"high":4,"super":5}""".decode('utf8')
    print >> mainFuction,"""//direction = {"auto":0,"vdir":1,"hdir":2}""".decode('utf8')
    print >> mainFuction,"""//mode = {"wind":0,"cool":1,"heat":2,"auto":3,"dehu":4}""".decode('utf8')
    print >> mainFuction,"""char feature[13][4]={"0.0", "0.0", "0.0", "2.0", "2.0", "3.0", "1.0", "3.0", "1.0", "2.0", "2.0", "2.0", "9.0"};""".decode('utf8')  
    print >> mainFuction,"""printf("Predict result for input feature: %d",(int)decisionTree(feature));""".decode('utf8')
    print >> mainFuction,"return 0;".decode('utf8')
    print >> mainFuction,"}".decode('utf8')
    
    print ('Conversion Success !')
    f1.close()
    f2.close()
    mainFuction.close()


# Convert Tree to JSON
def tree_json(tree,resultsFile):
    
    if os.path.exists(resultsFile):
        os.remove(resultsFile)
    
    data = []
    f1  = codecs.open(resultsFile,"a+","utf-8")
    for line in tree.splitlines() : 
        if line.strip():
            print >> f1,line.decode('utf8')
            line = line.strip()
            data.append(line)
        else : break
        if not line : break
    res = []
    res.append({'name':'Root', 'children':parse(data[1:])})
    
    with open('output/structure.json', 'w') as outfile:
        json.dump(res[0], outfile)
    print ('Conversion Success !')
    f1.close()
    outfile.close()
    
if __name__ == "__main__":
    
    dtModelFile = "output/DTModel"
    dtModelResults = "decisionTreeModel.txt"

    sc = SparkContext(appName="DecisionTreeClassification")
    dtModel = DecisionTreeModel.load(sc, dtModelFile)
    dtree = dtModel.toDebugString() 
    print dtree
    tree_C(dtree,dtModelResults)