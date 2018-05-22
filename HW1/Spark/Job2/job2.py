import datetime as dt
import csv
from pyspark import SparkContext
import time
sc =SparkContext()
rdd = sc.textFile("hdfs://localhost:9000/user/lollouno/input/Smallreviews.csv")
rdd = rdd.mapPartitions(lambda x: csv.reader(x))
start_time = time.time()

yearMap = ['NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA']


def reduce_function(line):
    lista = []
    year = dt.datetime.fromtimestamp(float(line[7])).strftime('%Y')
    score = line[6]
    productId = line[1]
    lista.append([year, productId, score])
    return lista


def mapping_function(line):
    year_score_list = line[1][2]
    year_score_string = ""
    for year_score in year_score_list:
        year_score_string += str(year_score) + " "
    year_score_string = year_score_string.strip()
    return (line[0]), year_score_string


def listing_function(a, b):
    year1 = a[0]
    year2 = b[0]
    avg1 = a[1]
    avg2 = b[1]
    list1 = a[2]
    list2 = b[2]
    list1[len(list1) - 1 - (2012 - int(year1))] = avg1
    list2[len(list2) - 1 - (2012 - int(year2))] = avg2
    for year in range(0, len(list1)):
        if list1[year] != 'NA':
            list2[year] = list1[year]
    return year2, avg2, list2


header = rdd.first()
tmp = rdd.filter(lambda line: line != header) \
    .flatMap(reduce_function) \
    .map(lambda line: ((line[1],line[0]), (int(line[2]),1))) \
    .reduceByKey(lambda a, b: ((a[0]*a[1]+b[0]*b[1])/(a[1]+b[1]), a[1]+b[1])) \
    .map(lambda line: ((line[0][0]), (line[0][1], line[1][0], yearMap.copy()))) \
    .filter(lambda line: (int(line[1][0]) >= 2003 and int(line[1][0]) <= 2012)) \
    .reduceByKey(listing_function) \
    .map(mapping_function) \
    .sortByKey()
tmp.saveAsTextFile("hdfs://localhost:9000/user/lollouno/output/Spark3Small")
print(str(start_time - time.time()))

# for i in tmp.take(200):
#     print(i)
