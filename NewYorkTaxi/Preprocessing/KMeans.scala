import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

def getTime(t:String): String = {
val hour = t.split(":")(0).toInt
var mins = t.split(":")(1).toInt
var converted = (mins/15) * 15
val time: String = hour+":"+converted
return time
}

def getDay(date: String): String = {
val t = date.substring(0, 10)
val d = t.split("-")
if(d(2).length == 1)
d(2) = "0" + d(2)
if(d(1).length == 1)
d(1) = "0" + d(1)
val parsedDate = d(2)+"/"+d(1)+"/"+d(0)
val dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy")
val dayOfWeek = LocalDate.parse(parsedDate,dtf).getDayOfWeek
val ret = dayOfWeek + ""
return ret
}

val FILEPATH = "/user/abm523/NewYorkTaxi/finalTaxiData/*"

val rdd = sc.textFile(FILEPATH)
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)

val ps = rdd1.map(line => line(0).split(" ")(0) + "," + line(0).split(" ")(1) + "," + line(2) + "," + line(4) + "," + line(5)).map(line => line.split(","))
val ds = rdd1.map(line => line(1).split(" ")(0) + "," + line(1).split(" ")(1) + "," + line(2) + "," + line(6) + "," + line(7)).map(line => line.split(","))

val pickups = ps.map(line => getDay(line(0)) + " " + getTime(line(1)) + "," + line(2) + "," + line(3) + "," + line(4))
val dropoffs = ds.map(line => getDay(line(0)) + " " + getTime(line(1)) + "," + line(2) + "," + line(3) + "," + line(4))

val m = pickups ++ dropoffs
val merged = m.map(line => line.split(","))
val grouped = merged.map(fields => (fields(0), (fields(1), fields(2), fields(3)))).groupByKey()
val keys = grouped.map(line => line._1)
val values = grouped.map(line => line._2)

val rawdense = values.map(line => line.map(attr => Vectors.dense(attr._2.toDouble, attr._3.toDouble)))

val clusters = KMeans.train(sc.parallelize(rawdense.take(3).flatMap(line=>line).toList), 1000, 10)
val centroids = clusters.clusterCenters
sc.parallelize(centroids).saveAsTextFile("project/centroids1000")

