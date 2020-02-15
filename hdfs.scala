import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Encoders}
import com.mongodb.spark._
import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql
import org.apache.spark._

import spark.implicits._
val df= spark.read.csv("hdfs://localhost:9000/mytable.csv").toDF
val df2= spark.read.csv("hdfs://localhost:9000/interesting.csv").toDF
df.show()
df2.show()
df.createOrReplaceTempView("mytable")
df2.createOrReplaceTempView("interesting")


val time2= System.nanoTime
val person2=  spark.sql("SELECT COUNT(*) FROM interesting")
person2.show()
println("Time taken: "+(System.nanoTime-time2)/1e6+"ms.")

val time3= System.nanoTime
val person3=  spark.sql("SELECT COUNT(*) FROM mytable WHERE _c1= 'WARN'")
person3.show()
println("Time taken: "+(System.nanoTime-time3)/1e6+"ms.")

val time4= System.nanoTime
val person4=  spark.sql("select COUNT(DISTINCT _c5) from mytable where _c4= ' api_client.rb' and _c1= 'WARN'")
person4.show()
println("Time taken: "+(System.nanoTime-time4)/1e6+"ms.")

val time5= System.nanoTime
val person5=  spark.sql("Select _c3, count(*) as c from mytable where _c6 like '%https://api.github.com%' group by _c3 order by c desc limit 10")
person5.show()
println("Time taken: "+(System.nanoTime-time5)/1e6+"ms.")

val time6= System.nanoTime
val person6=  spark.sql("Select _c3, count(*) as c from mytable where _c6 like '_Failed%' group by _c3 order by c desc limit 10")
person6.show()
println("Time taken: "+(System.nanoTime-time6)/1e6+"ms.")

val time7= System.nanoTime
val person7=  spark.sql("SELECT COUNT(*) as C, substring(tb._c2 from 11 for 3) as H FROM mytable as tb WHERE _c6 like '%https://%' GROUP BY H ORDER BY C DESC LIMIT 1")
person7.show()
println("Time taken: "+(System.nanoTime-time7)/1e6+"ms.")

val time8= System.nanoTime
val person8=  spark.sql("select count(*) as c, _c5 from mytable where (_c6 like '%api.github.com/repos/%') group by _c5 order by c desc limit 5")
person8.show()
println("Time taken: "+(System.nanoTime-time8)/1e6+"ms.")

val time9= System.nanoTime
val person9=  spark.sql("select substring(tb._c6 for 11 from position('Access' in tb._c6)+8 ) as fstrng,count(*) as cnt from mytable as tb where tb._c6 like '%Access:%' and tb._c6 like '%Failed request.%' group by fstrng order by cnt desc limit 1")
person9.show()
println("Time taken: "+(System.nanoTime-time9)/1e6+"ms.")

val time13= System.nanoTime 
val person13 = spark.sql("select count(*) from (select substring(_c5 from Position('/' in _c5) +1) as repp from mytable where (_c6 like '%api.github.com/repos/%')) as A inner join interesting as B on A.repp= B._c3")
person13.show()
println("Time taken: "+(System.nanoTime-time13)/1e6+"ms.")

val time14= System.nanoTime 
val person14 = spark.sql("select count(*) as c, substring(M._c5 from Position('/' in M._c5)+1) as repp from interesting as I, mytable as M where M._c6 like '_Failed%' and I._c3= substring(M._c5 from Position('/' in M._c5)+1) group by repp order by c desc limit 10")
person14.show()
println("Time taken: "+(System.nanoTime-time14)/1e6+"ms.")

