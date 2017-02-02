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


# Convert Tree to JSON
def tree_json(tree):
    
    if os.path.exists('decisionTreeModel.txt'):
        os.remove('decisionTreeModel.txt')
    
    data = []
    f1  = codecs.open('decisionTreeModel.txt',"a+","utf-8")
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

    sc = SparkContext(appName="DecisionTreeClassification")
    dtModel = DecisionTreeModel.load(sc, "output/DTModel")
    tree_to_json = dtModel.toDebugString() 
    print tree_to_json
    tree_json(tree_to_json)