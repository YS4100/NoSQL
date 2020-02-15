# Databricks notebook source
firstRDD=sc.textFile("/FileStore/tables/web_access_log.txt")
lines=firstRDD.filter(lambda x: len(x)>0).flatMap(lambda x: x.split("\n"))
img=[]
for line in lines.collect():
  method=line.split()[5]
  url=line.split()[6]
  i=url.rfind('.')
  if(i!=-1):
    fileE=url[i+1:]
    if (method=="\"GET"):
      if (fileE=="jpg"):
        img.append("JPG")
      elif(fileE=="gif"):
        img.append("GIF")
      else:
        img.append("OTHERS")
        
if(len(img)):
  imgRDD = sc.parallelize(img)
  imgCounter = imgRDD.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortByKey(True)
  
print(imgCounter.collect())


# COMMAND ----------

firstRDD=sc.textFile("/FileStore/tables/web_access_log.txt")
lines=firstRDD.filter(lambda x: len(x)>0).flatMap(lambda x: x.split("\n"))
month=[]
for line in lines.collect():
  time=line.split()[3]
  byte=line.split()[9]
  i=time.find('/')
  if(byte=='-'):
    month.append([time[i+1:i+4],0])
  else:
    month.append([time[i+1:i+4],int(byte)])

if(len(month)):
  reqRDD = sc.parallelize(month)
  req = reqRDD.map(lambda x: (x[0],[x[1]/1024,1])).reduceByKey(lambda x,y: [x[0]+y[0],x[1]+y[1]])
  
print(req.collect())



# COMMAND ----------

firstRDD=sc.textFile("/FileStore/tables/web_access_log.txt")
lines=firstRDD.filter(lambda x: len(x)>0).flatMap(lambda x: x.split("\n"))
time=[]
for line in lines.collect():
  status = line.split()[8]
  if(status=="404"):
    timeS=line.split()[3]
    zone=line.split()[4]
    url=line.split()[10]
    time.append([timeS + " " + zone,url])

if(len(time)):
  timeRDD = sc.parallelize(time)
  time404 = timeRDD.map(lambda x: (x[0],x[1]))

print(time404.collect())


# COMMAND ----------


