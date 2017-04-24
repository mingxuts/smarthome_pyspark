'''
Created on 2 Feb 2017

@author: xuepeng
'''

import codecs
import json
import os
from pyspark import SparkContext
from pyspark.mllib.tree import DecisionTreeModel, RandomForestModel


paths = ''
# Parser


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
    if os.path.exists('output/structure_rule.txt'):
        os.remove('output/structure_rule.txt')
    
    walk(res,'')
    


def walk(list1, path = ""):
    
    f2  = codecs.open('output/structure_rule.txt',"a+","utf-8") 
    
    for dic in list1:
        #print('about to walk', dic['name'], 'passing path -->', path)
        if(len(dic['children']) == 1):
            paths = path+dic['name']+':'+dic['children'][0]['name']
                    
            paths = paths\
                    .replace('.0','')\
                    .replace('feature ','')\
                    .replace('Predict: ','#')\
                    .replace('not in','5')\
                    .replace(' ','|')\
                    .replace('in','4')\
                    .replace('>=','0')\
                    .replace('<=','1')\
                    .replace('>','2')\
                    .replace('<','3')\
                    .replace('Root:','')
            print >> f2, paths.decode('utf8') 

        else:
            walk(dic['children'], path+dic['name']+':')
            
           

    f2.close()
    
if __name__ == "__main__":
    
    dtModelFile = "output/RFModel"
    dtModelResults = "randomForestModel.txt"

    sc = SparkContext("local[20]","RFClassification")
    dtModel = RandomForestModel.load(sc, dtModelFile)
    dtree = dtModel.toDebugString() 
    print dtree
    tree_json(dtree,dtModelResults)