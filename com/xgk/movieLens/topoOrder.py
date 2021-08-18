from queue import Queue


# ===============================================================================
# 合并相似标签
# ===============================================================================
def topoOrder(edgeset, threshold, tagNum):
    # edgeset：((tag2idx, tag1idx), sim)
    # tagNum：所有标签个数

    # 定义各种变量，每个顶点的活跃度设置为1，入度的顶点集合初始化为0
    print('edgeset len' + str(len(edgeset)))
    # adjList：[[(tag1idx, sim)]]邻接矩阵，记录每个标签的出度，包括连接的标签及两者的相似度
    adjList = [[] for i in range(tagNum)]
    # indegree：每个标签的入度，生成tagNum个的0队列
    indegree = [0] * tagNum
    # activity：每个标签的活跃度
    activity = [1] * tagNum
    # que：用于维护入度为0的标签队列
    que = Queue()

    # 统计标签的活跃度和入度的顶点集合
    for ((tag2idx, tag1idx), sim) in edgeset:
        # 对每个标签的入度进行初始化
        indegree[tag1idx] += 1
        # 添加出度信息
        adjList[tag2idx].append((tag1idx, sim))
    print("indegree", indegree)
    for i in range(len(indegree)):
        if indegree[i] == 0:
            que.put(i)

    # covered：结果输出矩阵
    covered = [[] for i in range(tagNum)]

    # 如果有入度为0的顶点
    while not que.empty():
        # 从入度集合中取出一个为0的顶点
        vertexi = que.get()
        print('vertexi', vertexi)
        # 对的出度边集edgesi={(i,j)}按照相似度从大到小排序
        # edgeilist：[(tag1idx, sim), (), ...]
        edgeilist = sorted(adjList[vertexi], key=lambda tagSimTuple: tagSimTuple[1], reverse=True)
        print("e", edgeilist)
        # 如果没有出度，则再从入度集合中取出一个为0的顶点
        if len(edgeilist) == 0:
            continue
        # 依次取出edgesi中的每一条边(i,j)
        # 对每一条边根据前驱的活跃度和边的相似度与规定的阈值进行比较
        for i in range(len(edgeilist)):
            vertexj = edgeilist[i][0]
            sim = edgeilist[i][1]
            if sim * activity[vertexi] < threshold:
                indegree[vertexj] -= 1
                if indegree[vertexj] == 0:
                    que.put(vertexj)
                continue
            else:
                # vertexi 加入vertexj
                tmpSet = covered[vertexi] + [vertexi]#tmpSet:[vertexi]
                covered[vertexj] += tmpSet
                covered[vertexi] = []
                # 更新vertexj活跃度
                tmpActivity = activity[vertexi] * sim
                if tmpActivity < activity[vertexj]:
                    activity[vertexj] = tmpActivity
                # vertexj 减入度
                indegree[vertexj] -= 1
                if indegree[vertexj] == 0:
                    que.put(vertexj)
                activity[vertexi] = 0
    return covered


if __name__ == '__main__':
    edgeset = (
    ((1, 2), 0.4), ((1, 3), 0.6), ((1, 4), 0.5), ((6, 4), 0.8), ((6, 5), 0.73), ((4, 5), 0.62), ((3, 2), 0.92),
    ((3, 5), 0.21))
    threshold = 0.5
    tagNum = 7
    print(topoOrder(edgeset, threshold, tagNum))