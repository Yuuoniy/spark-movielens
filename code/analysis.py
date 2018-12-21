
from pyspark import SparkConf, SparkContext
import numpy as np
import matplotlib.pyplot as plt
conf = SparkConf().setMaster("local[*]").setAppName("First_App")
sc = SparkContext(conf=conf)

# 载入用户数据并解析
user_data = sc.textFile("../data/users.dat")
user_fields = user_data.map(lambda line: line.split("::"))

#计算用户数、职业数、邮编数
num_users = user_fields.map(lambda fields: fields[0]).count()
num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()
num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()


#计算年龄对应的人数
count_by_age = user_fields.map(lambda fields: (fields[2], 1)).reduceByKey(lambda x, y: x + y).collect()

#获取年龄和对应人数的数组，用来画图
Agex_axis = np.array([c[0] for c in count_by_age])
Agey_axis1 = np.array([c[1] for c in count_by_age])

plt.hist(ages, bins=20, color='lightblue', normed=True)
plt.rcParams['font.sans-serif']=['SimHei'] #解决中文乱码
plt.xticks([1,18,25,35,45,50,56])
plt.xlabel("年龄")
plt.ylabel("占比")
plt.show()

#统计职业对应的人数
count_by_occupation = user_fields.map(lambda fields: (fields[3], 1)).reduceByKey(lambda x, y: x + y).collect()

#获取职业和职业对应人数的数组，并且根据人数排序
x_axis1 = np.array([c[0] for c in count_by_occupation])
y_axis1 = np.array([c[1] for c in count_by_occupation])
x_axis = x_axis1[np.argsort(y_axis1)]
y_axis = y_axis1[np.argsort(y_axis1)]

pos = np.arange(len(x))
width = 1.0
ax = plt.axes()
ax.set_xticks(pos + (width / 2))
ax.set_xticklabels(x)
plt.xticks(rotation=30)
plt.bar(pos, y_axis, width, color='lightblue')
plt.show()


#载入电影数据集并解析
movie_data = sc.textFile("../data/movies.dat")
movie_fields = movie_data.map(lambda lines: lines.split("::"))

#获取影片的发行时间，非法则返回1900
def convert_year(x):
    try:
        return int(x[-5:-1])
    except:
        return 1900

years = movie_fields.map(lambda fields: fields[1]).map(lambda x: convert_year(x))

#过滤没有年份信息的电影
years_filtered = years.filter(lambda x: x != 1900).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()

#获取年份和年份对应电影数的数组，并且根据年份排序
Yearx_axis1 = np.array([c[0] for c in years_filtered])
Yeary_axis1 = np.array([c[1] for c in years_filtered])
y_axis2 = Yeary_axis1[np.argsort(Yearx_axis1)]
x_axis2 = Yearx_axis1[np.argsort(Yearx_axis1)]


#载入评分数据
rating_data = sc.textFile("../data/ratings.dat")
rating_fields = rating_data.map(lambda lines: lines.split("::"))


#利用map reduce 计算
ratings = rating_fields.map(lambda fields: int(fields[2]))
max_rating = ratings.reduce(lambda x, y: max(x, y))
min_rating = ratings.reduce(lambda x, y: min(x, y))
mean_rating = ratings.reduce(lambda x, y: x + y) / rating_data .count()
median_rating = np.median(ratings.collect())

print("最低评分: %d" % min_rating)
print("最高评分: %d" % max_rating)
print ("平均评分: %2.2f" % mean_rating)
print ("中位评分: %d" % median_rating)

