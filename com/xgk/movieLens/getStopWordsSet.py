from nltk.corpus import stopwords


# ===============================================================================
# 定义getStopWordsSet()函数
# ===============================================================================
def getStopWordsSet():
    #     nltk.download()

    # 调用stopwords中的英文停止词
    stop = set(stopwords.words('english'))
    # 在stopwords基础上添加新的停止词
    for i in range(32, 127):
        stop.add(chr(i))
        stop.add("'s")
        stop.add("l.")
        stop.add("\'\'")
        stop.add("movie")
        stop.add("best")
        stop.add("250")
        stop.add("100")
        stop.add("300")
        stop.add("top")
        stop.add("good")
        stop.add("based")
        stop.add("less")
        stop.add("film")
        stop.add("dvds")
        stop.add("films")
        stop.add("title")
        stop.add("book")
        stop.add("dvd")
    return stop