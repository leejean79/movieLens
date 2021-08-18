'''
Created on 2020年9月1日

@author: DELL
'''
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# 读取本地数据

#     配置spark环境
conf = SparkConf().setMaster("local").setAppName("tagTopic")
#     获取sparkcontext
sc = SparkContext(conf=conf)
#     读取本地文件
inputRdd = sc.textFile("./input/ratings.dat")

# 切分数据
trainSet, testSet = inputRdd.randomSplit([0.8, 0.2])

# 将rdd收集到driver端
trainSet = trainSet.collect()
testSet = testSet.collect()

# 保存数据到本地
trainSetFile = open("./output/trainRatings.dat", 'w')
testSetFile = open("./output/testRatings.dat", 'w')

for line in trainSet:
    trainSetFile.write(line + '\n')

for line in testSet:
    testSetFile.write(line + '\n')











