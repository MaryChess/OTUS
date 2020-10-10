###ДЗ2: Домашнее задание
# Анализ цен акций
# Цель: В данном ДЗ учимся читать/писать файлы на HDFS и использовать Spark RDD API для вычисления метрик.
# https://docs.google.com/document/d/1kbBFaWk0PMQFluJ67UUZpgBLC4chFF1ULKa6cedv7qI/edit?usp=sharing
# Критерии оценки: Выполнены пункты 2a, 2b - 2 балла
# Выполнен пункт 3 - 3 балла

from pyspark.sql import SparkSession
import multiprocessing

from datetime import datetime , timedelta
from pyspark.mllib.stat import Statistics

def MPars (a, b):
    try:
        a = float(a)
        b = float(b)
        return True
    except: return False
ss = SparkSession.builder.appName("OtusHW").getOrCreate()
sc = ss.sparkContext
initData = sc.textFile("/user/d.khoroshilov/all_stocks_5yr.csv")

refer = initData.first()

task_2_a = initData.filter(lambda  x: x != refer)\
    .map(lambda x: x.split(",")) \
    .filter(lambda  x: MPars(x[2],x[3]) )\
    .map(lambda x: ( x[6] ,  (float(x[2]), float(x[3])) ))\
    .groupByKey() \
    .map(lambda x: (x[0], list(x[1]))) \
    .map(lambda x: (x[0] ,  ( max([i[0] for i in x[1]])  , min([i[1] for i in x[1]])  ) ) )\
    .map(lambda x: (x[0] , x[1][0] - x[1][1]) )\
    .sortBy(keyfunc = lambda x: x[1], ascending=False)
   
    
def FDPars (a, b):
    try:
        a = datetime.strptime(a ,"%Y-%m-%d")
        b = float(b)
        return True
    except: return False  
    
def DateDiffer(arr):
    res = []
    for i in range(len(arr)):
        try:
           k = (arr[i][0] - arr[i-1][0], arr[i][1]/arr[i-1][1] - 1 )
           res += [k]
        except: res += [(timedelta(days = 5) , arr[i][1])]
    
    return res
    
def DateFilter(elem):
    return timedelta(days=1) == elem[0]
    

    
    
task_2_b = initData.filter(lambda  x: x != refer)\
    .map(lambda x: x.split(",")) \
    .filter(lambda  x: FDPars(x[0],x[4]) )\
    .map(lambda x: ( x[6] ,  (  datetime.strptime(x[0] ,"%Y-%m-%d") , float(x[4])) ))\
    .groupByKey()\
    .map(lambda x: (x[0], list(x[1]))) \
    .map(lambda x: (x[0], sorted(x[1]) )) \
    .map(lambda x: (x[0], DateDiffer(x[1]) )) \
    .map(lambda x: ( x[0], list(filter(DateFilter, x[1]))  ) )\
    .map(lambda x: ( x[0], max(x[1])[1] ))\
    .sortBy(keyfunc = lambda x: x[1], ascending=False)
    
    

    
    
task_3 = initData.filter(lambda  x: x != refer)\
    .map(lambda x: x.split(",")) \
    .filter(lambda  x: FDPars(x[0],x[4]) )\
    .filter(lambda  x: MPars(x[1],x[4]) )\
    .map(lambda x: ( x[6] ,  (  datetime.strptime(x[0] ,"%Y-%m-%d") , (float(x[1]) + float(x[4]))/ 2.0 ) )) \
    .groupByKey()\
    .map(lambda x: (x[0], list(x[1]))) \
    .cache()\
    
names  = task_3.map(lambda x: x[0]).collect()

num_names = len(names)
print("Total num of names: {} ".format(num_names) )

corrs = []
DAGs = []

# paired_names = []

# for i in range(num_names-1):

#     for j in range(i+1,num_names):
#         paired_names += [(names[i], names[j])]
        
# def calc_corr(names):
#     pir_collection = task_3.filter(lambda x : x[0] == names[0] or x[0] == names[1]).cache()
#     f_coll = pir_collection.filter(lambda x: x[0] == names[0]).flatMap(lambda x : x[1])
#     s_coll = pir_collection.filter(lambda x: x[0] == names[1]).flatMap(lambda x : x[1])

#     join = f_coll.leftOuterJoin(s_coll)\
#             .filter(lambda x: (x[1][0] != None ) and (x[1][1] != None) ).cache()

#     one = join.map(lambda x: x[1][0])
#     two = join.map(lambda x: x[1][1])
#     return ( (names[0], names[1]) , Statistics.corr(one,two))    
        
# paired_names = sc.parallelize(paired_names).map(calc_corr)        
        
        
for i in range(num_names-1):

    for j in range(i+1,num_names):
        pir_collection = task_3.filter(lambda x : x[0] == names[i] or x[0] == names[j])
        f_coll = pir_collection.filter(lambda x: x[0] == names[i]).flatMap(lambda x : x[1])
        s_coll = pir_collection.filter(lambda x: x[0] == names[j]).flatMap(lambda x : x[1])

        join = f_coll.leftOuterJoin(s_coll)\
            .filter(lambda x: (x[1][0] != None ) and (x[1][1] != None) )

        one = join.map(lambda x: x[1][0])
        two = join.map(lambda x: x[1][1])
        DAGs += [( (names[i], names[j]) , (one,two))]




        # corr = Statistics.corr(one,two)
        # corrs += [(corr, names[i], names[j])]    


# pir_collection = task_3.filter(lambda x : x[0] == names[0] or x[0] == names[1]).cache()
# f_coll = pir_collection.filter(lambda x: x[0] == names[0]).flatMap(lambda x : x[1])
# s_coll = pir_collection.filter(lambda x: x[0] == names[1]).flatMap(lambda x : x[1])

# join = f_coll.leftOuterJoin(s_coll)\
#     .filter(lambda x: (x[1][0] != None ) and (x[1][1] != None) ).cache()

# one = join.map(lambda x: x[1][0])
# two = join.map(lambda x: x[1][1])

# corr = Statistics.corr(one,two)
# corrs += [(corr, "name1", "name2")]



# shared_date = pir_collection\
#     .flatMap(lambda x: x[1])\
#     .map(lambda x: (x[0],1))\
#     .reduceByKey(lambda x,y : x+y)\
#     .filter(lambda x: x[1] == 2 )\
#     .map(lambda x: (x[0], 0 ))
    

  
    
# print(DAGs)

print(len(DAGs))


# pool = multiprocessing.Pool()
# corrs = list(pool.map(asa , range(9)))








corrs = sorted (list(map(lambda x: ( Statistics.corr(x[1][0],x[1][1]) , x[0]) , DAGs)) , reverse = True)[:3]

# corrs = paired_names.collect()

print(corrs)

t2_a = task_2_a.take(3)
t2_b = task_2_b.take(3)

answs = []

answs += ["2a-{},{},{}\n".format(t2_a[0][0],t2_a[1][0],t2_a[2][0])]
answs += ["2b-{},{},{}\n".format(t2_b[0][0],t2_b[1][0],t2_b[2][0])]
answs += ["3-{},{},{}\n".format( corrs[0][1], corrs[1][1]  ,corrs[2][1] )]


# with open("/user/d.khoroshilov/hw1/hw_1.txt", 'w') as file:
#     file.write("2a-{},{},{}\n".format(t2_a[0][0],t2_a[1][0],t2_a[2][0]))
#     file.write("2b-{},{},{}\n".format(t2_b[0][0],t2_b[1][0],t2_b[2][0]))
#     file.write("3-{},{},{}\n".format( corrs[0][1], corrs[1][1]  ,corrs[2][1]    ))

print("3-{},{},{}\n".format( corrs[0][1], corrs[1][1]  ,corrs[2][1]    ))

sc.parallelize(answs).saveAsTextFile("/user/d.khoroshilov/hw1/ans.txt")
