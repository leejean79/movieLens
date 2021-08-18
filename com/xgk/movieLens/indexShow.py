import matplotlib.pyplot as plt

#===============================================================================
# 读取指标文件 userTagResult
#===============================================================================
filepath = "./output/userTagResult"
file = open(filepath, 'r')
rowdata = []
for line in file:
    #print(line)
    line = line.split('\t')
    line = [w.strip() for w in line]
    rowdata.append((line[0], line[1], line[2]))

#===============================================================================
# 实现数据可视化
#===============================================================================
fig = plt.figure(1)
plt.title("Impact of each parameter")
plt.xlabel("Num of features (k)")
plt.ylabel("RMSE")#误差的均方根
lambda_ = [0.01, 0.1, 1.0]
color = ['r', 'g', 'b']

print(range(len(lambda_)))
for i in range(len(lambda_)):
    R = []
    RMSE = []
    for (r, l, rmse) in rowdata:
        r = int(r)
        l = float(l)
        rmse = float(rmse)
        if l == lambda_[i]:
            R.append(r)
            RMSE.append(rmse)
    plt.plot(R, RMSE, 'o' + color[i] + '-', label='lambda = ' + str(lambda_[i]))
plt.legend()
plt.show()