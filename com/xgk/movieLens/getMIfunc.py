"""
目的：计算标签相似度
知识点：计算俩个随机事件的平均值，两个随机事件归一化后的互信息
"""
from __future__ import print_function
from pyspark import SparkContext, SparkConf
from nltk import wordpunct_tokenize
import os
import math
import time
# import editDist
# import getStopWordsSet
# import topoOrder

#利用两个标签归一化的互信息作为两者的相似度
# 输入第一个参数为元组t：((tag1, tag2), (tag1prob, tag2prob, jointprob))
#jointprob为两标签的联合概率
#tag1prob为tag1的熵
# tag2prob为tag2的熵
def getMIfunc(t, itemNum):
    n = itemNum
    tag1=t[0][0]
    tag2=t[0][1]
    tag1prob=t[1][0]
    tag2prob=t[1][1]
    jointprob=t[1][2]
    # 计算两个随机事件的互信息
    MI = (
            math.log(float(n * jointprob) / float(tag1prob * tag2prob)) * jointprob / n
    )
    # 计算归一化后的两个随机事件的平均值
    normalizeFactor = (
            (tag1prob * math.log(n / float(tag1prob)) +
             tag2prob * math.log(n / float(tag2prob))) / (2 * n)
    )
    # 计算两个随机事件归一化后的互信息
    NMI = MI / normalizeFactor
    return ((tag1, tag2), NMI)




