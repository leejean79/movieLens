import time

import functools
from pyspark import SparkConf, SparkContext
import getStopWordsSet
from nltk import wordpunct_tokenize
import editDist
import filterFunc
from getMIfunc import getMIfunc
import topoOrder
import math
import mycmp

import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

"""
1 通过Spark读取本地电影标签数据input/tags.dat，对数据进行去停止词等处理
"""
conf = SparkConf().setMaster("local").setAppName("test")
spark = SparkContext(conf=conf)
inputRdd = spark.textFile("input/tags.csv")


# 1.1对本地的电影标签数据进行处理，将数据处理成(movieId ,tag)格式，并(movieId ,tag)格式数据去除停止词
#对数据本地的电影标签处理成(movieId ,tag)格式
input = inputRdd.map(
 lambda line: line.lower()
).map(
 lambda line: line.split(",")
).filter(
 lambda strlist: len(strlist) == 4
).map(
 lambda strlist: (strlist[0], strlist[1], strlist[2])
)

#t:(userId, movieId, tag)
input = input.map(
 lambda t: (t[1], t[2])
).filter(lambda t: t[0] not in "movieId")
# 1.2处理(movieId ,tag)格式数据，并去除停止词
stop = getStopWordsSet.getStopWordsSet()
# input:(movieId, tag)
input = input.map(
 #wordpunct_tokenize 单词标点分割，其中"isn't"被分割为"isn"，"'"  和“t"
 lambda t: (t[0], wordpunct_tokenize(t[1]))
).map(
 #t:(movieId, taglist)
  lambda t: (t[0], [" ".join(t[1])]) if len(t[1]) < 4 else (t[0], t[1])
).map(
 #t:(movieId, tags)
 lambda t: (t[0], [w for w in t[1] if w not in stop])
).flatMapValues(
 lambda x: x
)
# ('3265', 'heroic bloodshed')
# print(input.collect())

"""
2 计算两个标签之间的相似度
  编辑距离预处理，计算标签的相似度并保存
"""
#2.1编辑距离预处理

editdist_result = []
#编辑距离的阈值
editdist_threshold = 0.4
# 实现编辑距离的预处理，通过editDist函数计算两个标签的编辑距离
#对上面的电影标签数据取标签数据，去重，过滤
# input:(movieId, tags)
editDisInput = input.values().distinct().filter(
 lambda t: len(t) <= 10
).collect()
_total = len(editDisInput)
for i in range(len(editDisInput)):
  for j in range(len(editDisInput)):
    tag1 = editDisInput[i]
    tag2 = editDisInput[j]
    _max_len = editDist._max_operator(len(tag1), len(tag2))
    _min_len = editDist._min_operator(len(tag1), len(tag2))
    if _max_len == 0:
       continue
    if float(_max_len - _min_len) / float(_max_len) > editdist_threshold: # 利用下界剪枝
       continue
    _editdist = editDist.editDist(tag1, tag2)
    _sim = float(_editdist) / float(_max_len)
    if _sim < editdist_threshold:
     #调整标签顺序，保证前面的标签大于后面
      if not filterFunc.filterFunc(tag1, tag2):
        _tag = tag1
        tag1 = tag2
        tag2 = _tag
      editdist_result.append(((tag1, tag2), 1.0 - _sim))
  # print(float(i * _total + j) / float(_total * _total))

# 去重后movieId的数量
# input:(movieId, tags)
itemNum = input.keys().distinct().count()
# print(itemNum)
#标签的数量
tagNum = input.values().count()
#处理编辑预处理后的数据，并将数据聚类，生成标签与电影一一对应的数据
originalReverseInput = input.map(
 # t:(itemId, tag)
 lambda t: (t[1], int(t[0]))
)
# reverseInput:(tag, itemId)
reverseInput = originalReverseInput.distinct()
# aggregateByKey:aggregateByKey(初值)(分区内计算, 分区间计算)
# 初值只在分区内计算时参与
# key:tag
# 根据tag聚合movie
# reverseList:(tag, [movieId1,...,movieId2])
reverseList = reverseInput.aggregateByKey(
 [],
 lambda list1, item: list1 + [item],
 lambda list1, list2: list1 + list2
).map(
 # t:(tag, list1)
 lambda t: (t[0], sorted(t[1]))
)
# reverseList_local:[('chris evans', [34150, 53464]), .....]
reverseList_local = reverseList.collect()
# print(reverseList_local)
time_start = time.time()
print(time_start)

# 2.2计算标签相似度

MI_threshold = 0.6
joinReverseList_local = []
MIResultFilePath = "output/MIResult"
#编写相似度计算的算法，计算标签的相似度
MIResultFile = open(MIResultFilePath, 'w')
# reverseList_local:[('chris evans', [34150, 53464]), .....]
for (tag1, list1) in reverseList_local:
    for (tag2, list2) in reverseList_local:
        if filterFunc.filterFunc(tag1, tag2):
            # ((标签1，标签2），（标签1对应电影id列表长度，标签2对应电影id列表长度，两个电影列表的交集的长度））
            joinTuple = ((tag1, tag2), (len(list1), len(list2), len(filterFunc.listIntersection(list1, list2))))
            if joinTuple[1][2] == 0:
                continue
            #     计算互信息
            # MITuple：((tag1, tag2), NMI)
            MITuple = getMIfunc(joinTuple, itemNum)
            if MITuple[1] > MI_threshold:
                joinReverseList_local.append(MITuple)
#将标签相似度保存在output/MIResult文件
time_end = time.time()
print(time_end - time_start)
for ((tag1, tag2), sim) in joinReverseList_local:
    MIResultFile.write(str(tag1) + "::" + str(tag2) + "::" + str(sim) + "\n")
MIResultFile.close()

"""
3.合并相似的标签
"""
# 使用topoOrder函数合并相似的标签

# 3.1数据预处理
# reverseList:(tag, [movieId1,...,movieId2])
#zipWithIndex():该函数将RDD中的元素和这个元素在RDD中的ID（索引号）组合成键/值对。
# reverseInput:(tag, itemId)
# tagIdx:[(tag1, 0), (tag2, 1), ...]
tagIdx = reverseInput.keys().distinct().zipWithIndex()
# joinReverseList_local: [((tag1, tag2), NMI), ...]
topoinput = spark.parallelize(joinReverseList_local)
topoinput = topoinput.map(
 # t:((tag1, tag2), sim)
 lambda t: (t[0][0], (t[0][1], t[1]))
#  join后(tag1, ((tag2, sim), 0))
).join(tagIdx).map(
 # t:(tag1, ((tag2, sim), tag1idx))
 lambda t: (t[1][0][0], (t[1][1], t[1][0][1])) #(tag2, (tag1idx, sim))
).join(tagIdx).map(
 # t:(tag2, ((tag1idx, sim), tag2idx))
 lambda t: ((t[1][0][0], t[1][1]), t[1][0][1])
)
# topoinput:[((tag1idx, tag2idx), 0.6234832567815315), ...]
# print(topoinput.collect())

# 3.2 利用topoOrder()函数合并标签
topoinput = topoinput.collect()
# tagIdx:[(tag1, 0), (tag2, 1), ...]
inverseTagIdx = tagIdx.map(
 # t:(tag, tagidx)
 lambda t: (t[1], t[0])
)
#利用topoOrder()函数合并标签
# topoResult:[[2,4],[5],...]
topoResult = topoOrder.topoOrder(topoinput, MI_threshold, tagIdx.count())
topoResult = spark.parallelize(topoResult)
topoResult = topoResult.zipWithIndex().map(
 # t:(list1, idx)
 lambda t: (t[1], t[0])
).filter(
 # t:(idx, list1)
 lambda t: not len(t[1]) == 0
)
# topoResult:[(4, [3]), (9, [474]), (16, [17]), (18, [19]), (22, [21]), (27, [1369, 29, 1368]), ...]
# print(topoResult.collect())

"""
4 生成标签与标准标签对应并存储
"""
# 4.1 生成标签与标准标签对应
topoResult = topoResult.flatMapValues(
 lambda x: x
 # [(4, 3), (9, 474), (16, 17), ...] join (tagidx, tag) = (stagidx, (tagidx, stag))
).join(inverseTagIdx).map(
 lambda t: (t[1][0], t[1][1]) # (tagidx, stag) join (tagidx, tag) = (tagidx, (stag, tag))
).join(inverseTagIdx).map(
 lambda t: (t[1][1], t[1][0])
)
topoResult_local = topoResult.collect()

# 4.2将标签与标准标签对应关系存储在本地的output/standardTagResult文件
standardTagResultFilePath = "output/standardTagResult"
standardTagResultFile = open(standardTagResultFilePath, 'w')
for (tag, stag) in topoResult_local:
 standardTagResultFile.write(str(tag) + "::" + str(stag) + "\n")
standardTagResultFile.close()

#4.3生成并存储(mid::tag::freq)格式的数据
# replacedInput: ((tag1, tag2), sum和)
replacedInput = input.map(
 # input:(itemId, tag)
 lambda t: (t[1], t[0])
#  input:(tag, itemId) leftOUterJoin (tag1, tag2)
#  leftOuterJoin类似于sql的leftjoin：(tag, (mid, stag))
).leftOuterJoin(topoResult).map(
 # 形成((tag1, tag2), 1)
 lambda t: ((t[1][0], t[1][1]), 1) if t[1][1] is not None else ((t[1][0], t[0]), 1)
).reduceByKey(
 lambda i, j: i + j
)
#将((mid, tag), freq)格式格式数据转变成(mid::tag::freq)格式，并把数据存储在本地的output/movieStandardTag.dat文件
replacedInput_local = replacedInput.collect()
movieTagFilePath = "output/movieStandardTag.dat"
movieTagFile = open(movieTagFilePath, 'w')
for ((mid, tag), freq) in replacedInput_local:
 movieTagFile.write(str(mid) + "::" + str(tag) + "::" + str(freq) +"\n")
movieTagFile.close()
# 5690::tragic::1

"""
5.用户聚集标签
"""
# 5.1读取本地数据
movieTagFilePath = "output/movieStandardTag.dat"
movieTagFile = open(movieTagFilePath, 'r')
movieTaginput_local = []
for line in movieTagFile:
    line = line.lower()
    linelist = line.split("\n")[0].split("::")
    movieTaginput_local.append((int(linelist[0]), (linelist[1], float(linelist[2]))))
movieTagFile.close()

#读取本地的input/ratings.dat，并去除数据中的\n和::符号
# links数据解释：（movieId，imdbId, tmdbId):
# movieId:电影在movielens上的id，通过https://movielens.org/movies/(movieId)访问
# imdbId:电影在imdb网站上的id，通过http://www.imdb.com/title/(imdbId)访问
# tmdbId:电影在themoviedb上的id，通过http://www.imdb.com/title/(tmdbId)访问
movieRateFilePath = "./input/ratings.dat"
movieRateFile = open(movieRateFilePath, 'r')
movieRateInput_local = []
for line in movieRateFile:
    line = line.lower()
    linelist = line.split("::")
    # (userid, movieid, rate)
    movieRateInput_local.append((int(linelist[1]), (int(linelist[0]), float(linelist[2]))))
    # movieRateInput_local.append((int(linelist[0]), (linelist[1], float(linelist[2]))))
print("load finished")

userTagRate_local = []
#使用sorted函数对数据进行排序
# t:(mid, (tag, freq))
movieTaginput_local = sorted(movieTaginput_local, key=lambda t: t[0])
# t:(mid, (userid, rate))
movieRateInput_local = sorted(movieRateInput_local, key=lambda t: t[0])

print("start join")

# 实现将电影标签聚集到用户上
i = 0
j = 0
while i < len(movieTaginput_local) and j < len(movieRateInput_local):
    #寻找两个变量中相同电影分别对应的序号
    (mid1, (tag1, freq1)) = movieTaginput_local[i]
    (mid2, (userid2, rate2)) = movieRateInput_local[j]
    if mid1 < mid2:
        i += 1
    elif mid1 > mid2:
        j += 1
    elif mid1 == mid2:
        # sub-routine:
        i1 = i
        j1 = j
        back = j1
        while True:
            #通过算法，movieTag与movieRate聚集在一起，其中rate=_freq1 * _rate2
            while j1 < len(movieRateInput_local) and movieTaginput_local[i1][0] == movieRateInput_local[j1][0]:
                (_mid1, (_tag1, _freq1)) = movieTaginput_local[i1]
                (_mid2, (_userid2, _rate2)) = movieRateInput_local[j1]
                userTagRate_local.append(((_userid2, _tag1), _freq1 * _rate2))
                j1 += 1
            if i1 + 1 < len(movieTaginput_local):
                if movieTaginput_local[i1 + 1][0] == movieRateInput_local[j1 - 1][0]:
                    i1 += 1
                    j1 = back
                    continue
                else:
                    break
            else:
                break
        i = i1
        j = j1
print("finish join")

# print(userTagRate_local)

del movieTaginput_local
del movieRateInput_local

"""
5 归一化用户标签
"""
#按标签大小对数据进行排序
# python3中sorted函数的cmp参数被移除了，必须传入一个key，所以引入functools.cmp_to_key
# ((_userid2, _tag1), _freq1 * _rate2)
userTagRate_local = sorted(userTagRate_local, key=functools.cmp_to_key(mycmp.mycmp))
_userTagRate_local = []
_userTagRate_local.append(userTagRate_local[0])
p = 0
#实现了对(userId, tag, rate)中的rate进行归一化，即将(userId, tag, rate)中(userId, tag)相同的分数rate相加
for i in range(1, len(userTagRate_local)):
 ((userid, tag), rate) = userTagRate_local[i]
 # usrid和tag都相等
 if mycmp.mycmp(((userid, tag), rate), _userTagRate_local[p]) == 0:
   _sum = _userTagRate_local[p][1]
   _userTagRate_local[p] = ((userid, tag), rate + _sum)
 else:
   #   如果不一致，放入临时列表等待下一轮的比较
   _userTagRate_local.append(userTagRate_local[i])
   p += 1
#      最终用户和标签一致的都进行了累加
userTagRate_local = _userTagRate_local
# 保存归一化后的数据
userTagRateFilePath = "output/userTagRate.dat"
userTagRateFile = open(userTagRateFilePath, 'w')
for ((userid, tag), rate) in userTagRate_local:
  userTagRateFile.write(str(userid) + '::' + str(tag) + '::' + str(rate) + '\n')
userTagRateFile.close()
print("finished")

"""
6 生成用户的标签词云图
"""
# 通过读取userTagRate文件，获取某个用户对应的所有标签，使用WordCloud生成用户的词云图

# 读取数据
#使用SparkCon来设置这个Spark程序的配置项，使用setAppNeme(String appName)方法设置程序的名字为“test”,使用setMaster()方法设置Spark程序的运行模式为local(本地模式)，并使用spark来读取本地的output/userTagRate.dat数据。
# conf = SparkConf().setMaster("local").setAppName("test")
# spark = SparkContext(conf=conf)
userTagRateinput = spark.textFile("output/userTagRate.dat")
userTagRateinput = userTagRateinput.map(
 #    line：1::autism::5.0
 lambda line: line.split("::")
).map(
 lambda strlist: (int(strlist[0]), (strlist[1], float(strlist[2]))) # (uid, (tag, rate))
)


# 获取一个用户的所有标签
#使用aggregateByKey函数获得用户对应的标签
# t：(tag, rate)
# userTagRateList:[(uid1, [(tag1, rate1), (tag2 rate2), ...]), (uid2, [(tag1, rate1), (tag2 rate2), ...)]), .....]
userTagRateList = userTagRateinput.aggregateByKey(
 [],
 lambda list, t: list + [(t[0], t[1])],
 lambda list1, list2: list1 + list2
)
userTagRateList_local = userTagRateList.collect()
#选择编号为593的用户，并获取该用户对应的标签。
# user1Info:(593, [(tag1, rate1), (tag2 rate2), ...])
user1Info = userTagRateList_local[593]
# user1TagList:[(tag1, rate1), (tag2, rate2), ...]
user1TagList = user1Info[1]
# t: (tag, rate)根据rate排序
user1TagList = sorted(user1TagList, key=lambda t: t[1])
print(user1TagList)
tagDic = {}
percent = 0
# 构建key为tag，value为rate的字典
for (tag, rate) in user1TagList:
 tagDic[tag] = rate

 # 生成用户标签的词云
 input_img_path = "input/background.jpg"
 backgound_img = plt.imread(input_img_path)
 # 使用WordCloud函数生成词云图
 wc = WordCloud(background_color='white', mask=backgound_img)
 wc.generate_from_frequencies(tagDic)

 # 使用pyplot显示词云
 plt.imshow(wc)
 plt.axis('off')
 plt.show()