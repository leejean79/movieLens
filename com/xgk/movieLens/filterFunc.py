"""
标签过滤功能,输入tag2，tag1，
当len(tag2)>len(tag1),or tag2>tag1返回true
"""
def filterFunc(tag2, tag1):
    if len(tag1) < len(tag2):
        return True
    elif len(tag1) > len(tag2):
        return False
    elif tag1 == tag2:
        return False
    elif tag1 < tag2:
        return True
    else:
        return False

# listIntersection()函数实现求解两个有序表的交集的功能，主要记录两个有序表相同的字符
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
