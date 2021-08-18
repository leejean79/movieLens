# ===============================================================================
# 定义_min_operator()函数
# _min_operator()函数实现了取两个数之间的最小值。
# ===============================================================================
def _min_operator(a, b):
    if a < b:
        return a
    else:
        return b


# ===============================================================================
# 定义_max_operator()函数
# _max_operator()函数实现了取两个数之间的最大值
# ===============================================================================
def _max_operator(a, b):
    if a > b:
        return a
    else:
        return b


# ===============================================================================
# 定义editDist()函数
# ===============================================================================
def editDist(str1, str2):
    _str1 = ""
    _str2 = ""
    front = 0
    back = 0
    # 从前面开始计算出单词之间字母开始不同的最小位置
    for i in range(_min_operator(len(str1), len(str2))):
        if str1[i] == str2[i]:
            front += 1
        else:
            break
    # 从后面开始计算出单词之间字母开始不同的最大位置
    for i in range(_min_operator(len(str1), len(str2))):
        if str1[len(str1) - i - 1] == str2[len(str2) - i - 1]:
            back += 1
        else:
            break
    # 将两个单词的相同的位置相同的字母去掉
    for i in range(front, len(str1) - back):
        _str1 += str1[i]
    for i in range(front, len(str2) - back):
        _str2 += str2[i]

    _max = 100000
    # 为了简单，在字符串见面加空格，使字符串从1开始
    str1 = " " + _str1
    str2 = " " + _str2
    # 动态规划表
    # str1行，str2列的矩阵
    dp = [[_max for j in range(len(str2))] for i in range(len(str1))]
    # 设置初始状态
    for i in range(len(str1)):
        dp[i][0] = i
    for j in range(len(str2)):
        dp[0][j] = j
    # 计算动态规划表中的各状态转移
    for i in range(1, len(str1)):
        for j in range(1, len(str2)):
            dp[i][j] = _min_operator(dp[i][j], dp[i - 1][j] + 1)
            dp[i][j] = _min_operator(dp[i][j], dp[i][j - 1] + 1)
            if str1[i] == str2[j]:
                dp[i][j] = _min_operator(dp[i][j], dp[i - 1][j - 1])
            else:
                dp[i][j] = _min_operator(dp[i][j], dp[i - 1][j - 1] + 1)
    # 返回词语编辑距离
    return dp[len(str1) - 1][len(str2) - 1]
