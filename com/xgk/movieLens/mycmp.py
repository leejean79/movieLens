#定义mycmp()函数，该函数实现比较两个标签大小的功能
def mycmp(t1, t2):
 ((userid1, tag1), rate1) = t1
 ((userid2, tag2), rate2) = t2
 userid1 = int(userid1)
 userid2 = int(userid2)
 if userid1 < userid2:
   return -1
 elif userid1 > userid2:
   return 1
 elif tag1 < tag2:
   return -1
 elif tag1 > tag2:
   return 1
 else:
   return 0
