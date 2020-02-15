import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Encoders}
import com.mongodb.spark._
import sys.process._
import org.apache.spark.sql._
import org.apache.spark._
import com.mongodb.spark.config._
import org.bson.Document
import scala.util.parsing.json.JSONObject
import org.apache.spark.sql



val spark= SparkSession.builder().appName("MongoSparkDataFrame").master("local[*]").config("spark.mongodb.input.uri","mongodb://127.0.0.1/mydb.interesting").config("spark.mongodb.output.uri","mongodb://127.0.0.1/mydb.interesting").getOrCreate()

val sc= spark.sparkContext

val readConfig = ReadConfig(Map("collection" -> "mytable", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
val readConfig2 = ReadConfig(Map("collection" -> "interesting", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))

val customRdd = MongoSpark.load(sc, readConfig)
val customRdd2 = MongoSpark.load(sc, readConfig2)

import spark.implicits._
val df = customRdd.toDF
val df2 = customRdd2.toDF
df.show()
df2.show()
df.createOrReplaceTempView("mytable")
df2.createOrReplaceTempView("interesting")


val time2= System.nanoTime 
val person2 = spark.sql("SELECT COUNT(*) FROM mytable")
person2.show()
println("Time taken: "+(System.nanoTime-time2)/1e6+"ms.")

val time3= System.nanoTime 
val person3 = spark.sql("SELECT COUNT(*) FROM mytable WHERE log_lvl= 'WARN'")
person3.show()
println("Time taken: "+(System.nanoTime-time3)/1e6+"ms.")

val time4= System.nanoTime 
val person4 = spark.sql("select COUNT(DISTINCT repo) from mytable where retrval_stage= ' api_client.rb' and log_lvl= 'WARN'")
person4.show()
println("Time taken: "+(System.nanoTime-time4)/1e6+"ms.")

val time5= System.nanoTime 
val person5 = spark.sql("Select dwnlder_id, count(*) as c from mytable where mssg like '%https://api.github.com%' group by dwnlder_id order by c desc limit 10")
person5.show()
println("Time taken: "+(System.nanoTime-time5)/1e6+"ms.")

val time6= System.nanoTime 
val person6 = spark.sql("Select dwnlder_id, count(*) as c from mytable where mssg like '_Failed%' group by dwnlder_id order by c desc limit 10")
person6.show()
println("Time taken: "+(System.nanoTime-time6)/1e6+"ms.")

val time7= System.nanoTime 
val person7 = spark.sql("SELECT COUNT(*) as C, substring(tb.timestamp from 11 for 3) as H FROM mytable as tb WHERE mssg like '%https://%' GROUP BY H ORDER BY C DESC LIMIT 1")
person7.show()
println("Time taken: "+(System.nanoTime-time7)/1e6+"ms.")

val time8= System.nanoTime 
val person8 = spark.sql("select count(*) as c, repo from mytable where (mssg like '%api.github.com/repos/%') group by repo order by c desc limit 5")
person8.show()
println("Time taken: "+(System.nanoTime-time8)/1e6+"ms.")

val time9= System.nanoTime 
val person9 = spark.sql("select substring(tb.mssg for 11 from position('Access' in tb.mssg)+8 ) as fstrng,count(*)as cnt from mytable as tb where mssg like '%Access:%' and mssg like '%Failed request.%' group by fstrng order by cnt desc limit 1")
person9.show()
println("Time taken: "+(System.nanoTime-time9)/1e6+"ms.")

val time11= System.nanoTime 
val person11 = spark.sql("Select COUNT(DISTINCT repo) from mytable where dwnlder_id like '%ghtorrent-22%'")
person11.show()
println("Time taken: "+(System.nanoTime-time11)/1e6+"ms.")

val time13= System.nanoTime 
val person13 = spark.sql("select count(*) from (select substring(repo from Position('/' in repo) +1) as repp from mytable where (mssg like '%api.github.com/repos/%')) as A inner join interesting as B on A.repp = B.nme")
person13.show()
println("Time taken: "+(System.nanoTime-time13)/1e6+"ms.")

val time14= System.nanoTime 
val person14 = spark.sql("select count(*) as c, substring(M.repo from Position('/' in repo)+1) as repp from interesting as I, mytable as M where M.mssg like '_Failed%' and I.nme= substring(M.repo from Position('/' in repo)+1) group by repp order by c desc limit 10")
person14.show()
println("Time taken: "+(System.nanoTime-time14)/1e6+"ms.")
