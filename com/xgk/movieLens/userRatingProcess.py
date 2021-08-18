
from __future__ import print_function
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.mllib.recommendation import ALS, Rating

import math

"""
I.读取本地数据
"""

inputpath_local_train = "./output/trainRatings.dat"
inputpath_local_test = "./output/testRatings.dat"

outputpath_local = "./output/userTagResult"

# 配置spark conf
conf = SparkConf().setMaster("local").setAppName("collaborative")
sc = SparkContext(conf=conf)

# 读入为rdd
trainSet = sc.textFile(inputpath_local_train)
testSet = sc.textFile(inputpath_local_test)

# 预处理
# (user_id, movie_id, rating)
trainSet = trainSet.map(
    lambda line: line.lower()
).map(
    lambda line: line.split('::')).map(
    lambda strlist: (strlist[0], strlist[1], strlist[2])
)
# 将集合中的str类型转为相应的类型
trainSet = trainSet.map(
    lambda t: (int(t[0]), int(t[1]), float(t[2]))
)

# 处理测试数据，去除文件数据中的Tab空格或::字符。
testSet = testSet.map(
    lambda line: line.lower()
).map(
    lambda line: line.split('::')).map(
    lambda strlist: (strlist[0], strlist[1], strlist[2]))
# 将训练数据变成((int(userId), int(itemId)), float(rating))格式，将测试数据变成(userId, itemId)格式。
testSet = testSet.map(
    lambda t: (int(t[0]), int(t[1]), float(t[2]))
)

origin_testSet = testSet.map(
    lambda t: ((int(t[0]), int(t[1])), float(t[2]))
)
# 准备好测试数据集
predictTestInput = testSet.map(
    lambda t: (t[0], t[1])
)

"""
II.基于ALS的协同过滤
"""
# 参数定义
_rank = [2, 4, 6, 8, 10, 12, 14, 16, 20, 25, 30]
_iterations = 10
_lambda = [0.01, 0.1, 1.0]
result = []

# 模型训练

for r in _rank:
    for l in _lambda:
        print(str(r) + " " + str(l))
        als = ALS()
        # 训练ALS算法模型
        alsModel = als.train(trainSet, r, iterations=_iterations, lambda_=l)
        # 预测测试数据的结果
        predictTestResult = alsModel.predictAll(predictTestInput).map(
            lambda t: ((int(t[0]), int(t[1])), float(t[2]))
        )

        # 计算预测数据的预测值与真实值之间的差距
        # =======================================================================
        # predictTestResult:((userId, movieId), predict)
        # =======================================================================
        # =======================================================================
        # origin_testSet:((userId, movieId), rating)
        # =======================================================================
        # =======================================================================
        # join后：((userId, itemId), (predict, rating))
        # =======================================================================
        predictTestResult = predictTestResult.join(origin_testSet).map(
            lambda t: (t[0], (float(t[1][0]) - float(t[1][1])) ** 2)
        )
        # 计算多个rank、lambda参数下的RMSE指标（预测结果评价指标）
        # 以下是RMSE指标计算的具体实现
        rmse = predictTestResult.values().collect()
        S = len(rmse)
        _sum = sum(rmse)
        rmse = math.sqrt(_sum / S)
        result.append((r, l, rmse))
        print("finished")
# 将rank、lambda与只对应的RMSE保存在result中。
result = sc.parallelize(result).map(
    lambda t: str(t[0]) + '\t' + str(t[1]) + '\t' + str(t[2])
)

result = result.collect()
outputFile = open(outputpath_local, 'w')
for line in result:
    outputFile.write(line + '\n')
outputFile.close()


