import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

def getTime(t:String): String = {
val hour = t.split(":")(0).toInt
var mins = t.split(":")(1).toInt
var converted = (mins/15) * 15
val time: String = hour+":" + converted
return time
}

def getDay(date: String): String = {
val t = date.substring(0, 10)
val d = t.split("-")
if(d(2).length == 1)
d(2) = "0" + d(2)
if(d(1).length == 1)
d(1) = "0" + d(1)
val parsedDate = d(2) + "/" + d(1) + "/"+d(0)
val dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy")
val dayOfWeek = LocalDate.parse(parsedDate,dtf).getDayOfWeek
val ret = dayOfWeek + ""
return ret
}

val FILEPATH = "/user/abm523/NewYorkTaxi/finalTaxiData/*"

val rdd = sc.textFile(FILEPATH)
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
//val ps = rdd1.map(line => line(0).split(" ")(0) + "," + line(0).split(" ")(1) + "," + line(2) + "," + line(4) + "," + line(5)).map(line => line.split(","))
val ds = rdd1.map(line => line(1).split(" ")(0) + "," + line(1).split(" ")(1) + "," + line(2) + "," + line(6) + "," + line(7)).map(line => line.split(","))
//val pickups = ps.map(line => getDay(line(0)) + " " + getTime(line(1)) + "," + line(2) + "," + line(3) + "," + line(4))
val dropoffs = ds.map(line => getDay(line(0)) + " " + getTime(line(1)) + "," + line(2) + "," + line(3) + "," + line(4))
//val m = pickups ++ dropoffs
val merged = dropoffs.map(line => line.split(","))
val grouped = merged.map(fields => (fields(0), (fields(1), fields(2), fields(3)))).groupByKey()
val keys = grouped.map(line => line._1)
val values = grouped.map(line => line._2)

val rawdense = values.map(line => line.map(attr => Vectors.dense(attr._2.toDouble, attr._3.toDouble)))
//rawdense.persist()
val numberOfClusters = 500

val temp = rawdense.take(17).drop(16)

val clusters = KMeans.train(sc.parallelize(temp.flatMap(line=>line).toList), numberOfClusters, 5)
val centroids = clusters.clusterCenters
val CLUSTERPATH = "/user/abm532/clusters2"
sc.parallelize(centroids).saveAsTextFile(CLUSTERPATH)



















import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
// Rounding off time to the nearest hour
def getTime(t:String): String = {
val hour = t.split(":")(0)
return hour
}

// Converting from the raw date format to Day of week format
def getDay(date: String): String = {
val d = date.split("-")
if(d(2).length == 1)
d(2) = "0" + d(2)
if(d(1).length == 1)
d(1) = "0" + d(1)
val parsedDate = d(2) + "/" + d(1) + "/" + d(0)
val dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy")
val dayOfWeek = LocalDate.parse(parsedDate,dtf).getDayOfWeek
val ret = dayOfWeek + ""
return ret
}
def getDay1(date: String): String = {
//to remove "(" at the start"
val t = date.substring(1,date.length)
val d = t.split("-")
if(d(2).length == 1)
d(2) = "0" + d(2)
if(d(1).length == 1)
d(1) = "0" + d(1)
val parsedDate = d(2) + "/" + d(1) + "/" + d(0)
val dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy")
val dayOfWeek = LocalDate.parse(parsedDate,dtf).getDayOfWeek
val ret = dayOfWeek + ""
return ret

}


def calcPeople(arr:Array[String]):String ={ 
     val i = arr(0).toInt
     var k = ""
     for(i <- 1 to i){
          k = k + arr(1)+" " + arr(2) + "," + arr(3) + ";"
     }

     return k
}

val k = merged.map(line => calcPeople(line))
val kk = k.map(line => line.split(";"))
val t = kk.flatMap(x => x)

val filepath = "/user/sjd451/project/cleanTaxiData"
val rdd = sc.textFile(filepath)
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
// date time lat long
val pickups = rdd1.map(line => getDay1(line(0).split(" ")(0)) + " " + getTime(line(0).split(" ")(1)) + "," + line(4) + "," + line(5)).map(line => line.split(","))
val drops = rdd1.map(line => getDay(line(1).split(" ")(0)) + " " + getTime(line(1).split(" ")(1)) + "," + line(6) + "," + line(7)).map(line => line.split(","))
// Collection of all pickups and dropoff latitudes and longitudes
val merged = pickups ++ drops
// Grouping latitudes and longitudes based on the timestamp
val grouped = merged.map(fields => (fields(0).split(" ")(1).toInt, (fields(1), fields(2)))).groupByKey()

val temp = grouped.lookup(0)
// Populating the RDD element into Dense Vectors
val b = grouped.map(line => (line._1, line._2.map(attr => Vectors.dense(attr._1.toDouble, attr._2.toDouble))))

// Passing dense vector to KMeans clustering algorithm
// Cluster size is set to 40
var i = 0

for(i <- 0 to 23){
val clusters = KMeans.train(sc.parallelize(b.lookup(i).flatMap(line=>line).toList), 40, 10)
val centroids = clusters.clusterCenters
sc.parallelize(centroids).saveAsTextFile("project/cluster-"+i)
}



Code to try:

val filepath = "/user/sjd451/project/cleanTaxiData"
val rdd = sc.textFile(filepath)
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
// date time lat long
val drops = rdd1.map(line => (getTime(line(1).split(" ")(1)) , line(6) , line(7)).sortByKey()
val toSave = drops.map(line=> line._1+","+line._2+","+line._3)
toSave.saveAsTextFile(<path>)
