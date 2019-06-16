import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

// Rounding off time to the nearest 15min mark 
def getTime(t:String): String = {
	val hour = t.split(":")(0).toInt
	var mins = t.split(":")(1).toInt
	var converted = (mins/15) * 15
	val time: String = hour+":"+converted
	return time
}

// Converting from the raw date format to Day of week format
def getDay(date: String): String = {
	import java.time.LocalDate
	import java.time.format.DateTimeFormatter
	val d = date.split("/")
	if(d(0).length == 1)
	d(0) = "0" + d(0)
	if(d(1).length == 1)
	d(1) = "0" + d(1)
	val parsedDate = d(0)+"/"+d(1)+"/"+d(2)
	val dtf = DateTimeFormatter.ofPattern("MM/dd/yyyy")
	val dayOfWeek = LocalDate.parse(parsedDate,dtf).getDayOfWeek
	val ret = dayOfWeek + ""
	return ret
}

val filepath = "/user/abm523/project/TD.csv"
val rdd = sc.textFile(filepath)
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
// date time lat long
val pickups = rdd1.map(line => getDay(line(1).split(" ")(0)) + " " + getTime(line(1).split(" ")(1)) + "," + line(5) + "," + line(6)).map(line => line.split(","))
val drops = rdd1.map(line => getDay(line(2).split(" ")(0)) + " " + getTime(line(2).split(" ")(1)) + "," + line(9) + "," + line(10)).map(line => line.split(","))
// Collection of all pickups and dropoff latitudes and longitudes
val merged = pickups ++ drops
// Grouping latitudes and longitudes based on the timestamp
val grouped = merged.map(fields => (fields(0), (fields(1), fields(2)))).groupByKey()
val keys = grouped.map(line => line._1)
val vals = grouped.map(line => line._2)
// Populating the RDD element into Dense Vectors
val b = vals.map(line => line.map(attr => Vectors.dense(attr._1.toDouble, attr._2.toDouble)))
// Passing dense vector to KMeans clustering algorithm
// Cluster size is set to 20
val clusters = KMeans.train(sc.parallelize(b.take(3).flatMap(line=>line).toList), 20, 10)
val centroids = clusters.clusterCenters
// Saving cluster centers to generate heat map denoting the density of vehicles at a particular location.
sc.parallelize(centroids).saveAsTextFile("project")