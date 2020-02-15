import java.util.Properties

:require /home/bansal/Downloads/postgresql-42.2.9.jar
Class.forName("org.postgresql.Driver") != null
val url = "jdbc:postgresql://localhost:5432/postgres"
val connectionProperties = new Properties()
connectionProperties.setProperty("Driver", "org.postgresql.Driver")
connectionProperties.setProperty("user", "postgres")
connectionProperties.setProperty("password", "abcdefg")

val time2= System.nanoTime 
val person2 = "(SELECT COUNT(*) FROM schemma_bda_1.mytable) as q1"
val personDf2 = spark.read.jdbc(url, person2, connectionProperties)
personDf2.show()
println("Time taken: "+(System.nanoTime-time2)/1e6+"ms.")

val time3= System.nanoTime 
val person3 = "(SELECT COUNT(*) FROM schemma_bda_1.mytable WHERE log_lvl= 'WARN') as q1"
val personDf3 = spark.read.jdbc(url, person3, connectionProperties)
personDf3.show()
println("Time taken: "+(System.nanoTime-time3)/1e6+"ms.")

val time4= System.nanoTime 
val person4 = "(select COUNT(DISTINCT repo) from schemma_bda_1.mytable where retrval_stage= ' api_client.rb' and log_lvl= 'WARN') as q1"
val personDf4 = spark.read.jdbc(url, person4, connectionProperties)
personDf4.show()
println("Time taken: "+(System.nanoTime-time4)/1e6+"ms.")

val time5= System.nanoTime 
val person5 = "(Select dwnlder_id, count(*) as c from schemma_bda_1.mytable where mssg like '%https://api.github.com%' group by dwnlder_id order by c desc limit 10) as q1"
val personDf5 = spark.read.jdbc(url, person5, connectionProperties)
personDf5.show()
println("Time taken: "+(System.nanoTime-time5)/1e6+"ms.")

val time6= System.nanoTime 
val person6 = "(Select dwnlder_id, count(*) as c from schemma_bda_1.mytable where mssg like '_Failed%' group by dwnlder_id order by c desc limit 10) as q1"
val personDf6 = spark.read.jdbc(url, person6, connectionProperties)
personDf6.show()
println("Time taken: "+(System.nanoTime-time6)/1e6+"ms.")

val time7= System.nanoTime 
val person7 = "(SELECT COUNT(*) as C, substring(tb.timestamp from 11 for 3) as H FROM schemma_bda_1.mytable as tb WHERE mssg like '%https://%' GROUP BY H ORDER BY C DESC LIMIT 1) as q1"
val personDf7 = spark.read.jdbc(url, person7, connectionProperties)
personDf7.show()
println("Time taken: "+(System.nanoTime-time7)/1e6+"ms.")

val time8= System.nanoTime 
val person8 = "(select count(*) as c, repo from schemma_bda_1.mytable where (mssg like '%api.github.com/repos/%') group by repo order by c desc limit 5) as q1"
val personDf8 = spark.read.jdbc(url, person8, connectionProperties)
personDf8.show()
println("Time taken: "+(System.nanoTime-time8)/1e6+"ms.")

val time9= System.nanoTime 
val person9 = "(select substring(tb.mssg for 11 from position('Access' in tb.mssg)+8 ) as fstrng,count(*)as cnt from schemma_bda_1.mytable as tb where mssg like '%Access:%' and mssg like '%Failed request.%' group by fstrng order by cnt desc limit 1) as q1"
val personDf9 = spark.read.jdbc(url, person9, connectionProperties)
personDf9.show()
println("Time taken: "+(System.nanoTime-time9)/1e6+"ms.")

val time10= System.nanoTime 
val person10 = "(Select COUNT(DISTINCT repo) from schemma_bda_1.tab where dwnlder_id like '%ghtorrent-22%') as q1"
val personDf10 = spark.read.jdbc(url, person10, connectionProperties)
personDf10.show()
println("Time taken: "+(System.nanoTime-time10)/1e6+"ms.")

val time11= System.nanoTime 
val person11 = "(Select COUNT(DISTINCT repo) from schemma_bda_1.mytable where dwnlder_id like '%ghtorrent-22%') as q1"
val personDf11 = spark.read.jdbc(url, person11, connectionProperties)
personDf11.show()
println("Time taken: "+(System.nanoTime-time11)/1e6+"ms.")

val time13= System.nanoTime 
val person13 = "(select count(*) from (select substring(repo from Position('/' in repo) +1) as repp from schemma_bda_1.mytable where (mssg like '%api.github.com/repos/%')) as A inner join schemma_bda_1.interesting as B on A.repp = B.nme) as q1"
val personDf13 = spark.read.jdbc(url, person13, connectionProperties)
personDf13.show()
println("Time taken: "+(System.nanoTime-time13)/1e6+"ms.")

val time14= System.nanoTime 
val person14 = "(select count(*) as c, substring(M.repo from Position('/' in repo)+1) as repp from schemma_bda_1.interesting as I, schemma_bda_1.mytable as M where M.mssg like '_Failed%' and I.nme= substring(M.repo from Position('/' in repo)+1) group by repp order by c desc limit 10) as q1"
val personDf14 = spark.read.jdbc(url, person14, connectionProperties)
personDf14.show()
println("Time taken: "+(System.nanoTime-time14)/1e6+"ms.")