"""
提取两个列表对应位置相同的字符
"""

def listIntersection(list1, list2):
    ret = []
    i = 0
    j = 0
    while i < len(list1) and j < len(list2):
        #记录两个有序表相同的字符
        if list1[i] == list2[j]:
            ret.append(list1[i])
            i += 1
            j += 1
            continue
        elif list1[i] < list2[j]:
            i += 1
            continue
        else:
            j += 1
            continue
    return ret