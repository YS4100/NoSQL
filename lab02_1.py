# Databricks notebook source
data = [1,2,3,4,5]
distData = sc.parallelize(data)

# COMMAND ----------

distFile = sc.textFile("data.txt")

# COMMAND ----------

firstRDD = sc.textFile("/FileStore/tables/test.txt")
lines = firstRDD.filter(lambda x: len(x)>0).flatMap(lambda x:x.split(" "))
wordcount = lines.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).sortByKey(True)
print(wordcount.take(8))

# COMMAND ----------

for word in wordcount.collect():
  print(word)

# COMMAND ----------

from pyspark.sql import *

department1 = Row(dno = '123456', name='Computer Science')
department2 = Row(dno= '789012', name='Mechanical Engineering')
department3 = Row(dno= '345678', name='Theater and Drama')
department4 = Row(dno= '901234', name='Indoor Recreation')

Employee = Row("ssn","firstName","lastName","email","salary","dno")
employee1 = Employee('999-555-666','michael','armbrust','no-reply@berkeley.edu',100000,"123456")
employee2 = Employee('336-225-646','xiangrui','meng','no-reply@stanford.edu',120000,"123456")
employee3 = Employee('215-555-999','matei',None,'no-reply@waterloo.edu',140000,"901234")
employee4 = Employee('222-555-666',None,'wendell','no-reply@berkeley.edu',160000,"345678")
employee5 = Employee('444-585-699','michael','jackson','no-reply@neverla.edu',80000,"789012")

# COMMAND ----------

employee_row_list = [employee1,employee2,employee3,employee4,employee5]
employee_df = spark.createDataFrame(employee_row_list)
employee_df.show()

dept_row_list = [department1,department2,department3,department4]
department_df = spark.createDataFrame(dept_row_list)

department_df.show()


# COMMAND ----------

employee_df.join(department_df,employee_df.dno==department_df.dno,'inner').show()
employee_df.registerTempTable('Employee')
department_df.registerTempTable('Department')
result_df=sqlContext.sql("select * from Employee as e join Department as d on e.dno=d.dno")
result_df.show()


# COMMAND ----------

diamonds=spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",header="true",inferSchema="true")
diamonds.write.format("delta").save("/delta/diamonds")

# COMMAND ----------


