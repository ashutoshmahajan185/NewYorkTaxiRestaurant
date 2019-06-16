val CLUSTERS_PART = "/user/abm523/NewYorkTaxi/KMeans1000"
val RESTAURANTDATA_PART = "/user/abm523/NewYorkTaxi/restaurantData/part-00000"
val CUISINEDATA_PART = "/user/abm523/NewYorkTaxi/cuisineData/part-00000"
val CLEAN_FOLDERPATH = "/user/abm523/NewYorkTaxi/finalTaxiData"
val GEOPATH = "/user/abm523/taxiMyRestaurant/restaurantData/resLatLon.txt"
val LATLONRDD = sc.textFile(GEOPATH)
val llrdd = LATLONRDD.filter(line => !line.isEmpty).map(line => line.split(","))
val llrddkey = llrdd.map(line => (line(0), (line(1), line(2)))).distinct

def degToRad(degree: Double): Double = {
	return degree * (Math.PI/180)
}

// Calculate the displacement between two points
def getDisplacement(lat1: Double, lat2: Double, lon1: Double, lon2: Double): Double = {
	val radius = 6371;
	val disLat = degToRad(lat2-lat1);
	val disLon = degToRad(lon2-lon1);
	val temp1 = Math.sin(disLat/2) * Math.sin(disLat/2) + Math.cos(degToRad(lat1)) * Math.cos(degToRad(lat2)) * Math.sin(disLon/2) * Math.sin(disLon/2); 
	val temp2 = 2 * Math.atan2(Math.sqrt(temp1), Math.sqrt(1-temp1)); 
	var distance = radius * temp2;
	return distance;
}

def calcDensity(lat:Double, lon:Double): Long = {
	val clusters = sc.textFile(CLUSTERS_PART).map(_.replace("]","").replace("[","").split(",")).filter(line => getDisplacement(line(1).toDouble, lat, line(0).toDouble, lon) < 0.5)
	return clusters.count
}

val resData = sc.textFile(RESTAURANTDATA_PART).map(_.split("\t"))
val clusters = sc.textFile(CLUSTERS_PART)
val resData2 = resData.keyBy(line => line(1))
println("Enter the restaurant name")
//var restaurant = Console.readLine()
var restaurant = "MERCI MARKET"
//40.638431, -74.034668
println("Enter your current Latitude")
//var userLat = Console.readLine().toDouble
var userLat = 40.638431
println("Enter your current Longitude")
//var userLon = Console.readLine().toDouble
var userLon = -74.034668
val details = resData2.lookup(restaurant)
val lat = details.take(1)(0)(3).toDouble
val lon = details.take(1)(0)(4).toDouble
val userDist = getDisplacement(lat, userLat, lon, userLon)
var density = calcDensity(lat,lon)

 
if(density > 3) {

	println("The restaurant you are trying to reach may be busy")
	println("Enter the desired restaurant rating")
	//var userRating = Console.readLine()
	var userRating = "A"
	val cuisines = sc.textFile(CUISINEDATA_PART)
	val b = cuisines.keyBy(x => x.split("\t")(0))
	val c = b.map(line => (line._1, line._2.split("\t").drop(1)))
	
	var cuisine = "American"
	val alternate = c.lookup(details.take(1)(0)(6))
	println("ithe pochlo")
	//check if the lat and lon are correctly placed
	val hereYouGo = alternate.flatMap(x => x).filter(x => x.split(",").length == 3).filter(x => getDisplacement(userLat, x.split(",")(1).toDouble,  userLon, x.split(",")(2).toDouble) <= userDist)
	val hyg = sc.parallelize(hereYouGo).map(_.split(","))

	val h = hereYouGo.filter(x => calcDensity(x(2).toDouble, x(1).toDouble) < 3)
	//import model, predict the fare to the model, take least 10 fares.
	println("hereYouGo")

}
































import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType};
import org.apache.spark.sql.Row;
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.{Pipeline, PipelineModel}


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

def degToRad(degree: Double): Double = {
return degree * (Math.PI/180)
}

// Calculate the displacement between two points
def getDisplacement(lat1: Double, lat2: Double, lon1: Double, lon2: Double): Double = {
val radius = 6371;
val disLat = degToRad(lat2-lat1);
val disLon = degToRad(lon2-lon1);
val temp1 = Math.sin(disLat/2) * Math.sin(disLat/2) + Math.cos(degToRad(lat1)) * Math.cos(degToRad(lat2)) * Math.sin(disLon/2) * Math.sin(disLon/2); 
val temp2 = 2 * Math.atan2(Math.sqrt(temp1), Math.sqrt(1-temp1)); 
var distance = radius * temp2;
return distance;
}


val CLUSTERS_PART = "/user/abm523/NewYorkTaxi/KMeans1000"
val RESTAURANTDATA_PART = "/user/abm523/NewYorkTaxi/restaurantData/part-00000"
val CUISINEDATA_PART = "/user/abm523/NewYorkTaxi/cuisineData/part-00000"
val CLEAN_FOLDERPATH = "/user/abm523/NewYorkTaxi/finalTaxiData"
val GEOPATH = "/user/abm523/taxiMyRestaurant/restaurantData/resLatLon.txt"
val LATLONRDD = sc.textFile(GEOPATH)
val llrdd = LATLONRDD.filter(line => !line.isEmpty).map(line => line.split(","))
val llrddkey = llrdd.map(line => (line(0), (line(1), line(2)))).distinct

def degToRad(degree: Double): Double = {
return degree * (Math.PI/180)
}

// Calculate the displacement between two points
def getDisplacement(lat1: Double, lat2: Double, lon1: Double, lon2: Double): Double = {
val radius = 6371;
val disLat = degToRad(lat2-lat1);
val disLon = degToRad(lon2-lon1);
val temp1 = Math.sin(disLat/2) * Math.sin(disLat/2) + Math.cos(degToRad(lat1)) * Math.cos(degToRad(lat2)) * Math.sin(disLon/2) * Math.sin(disLon/2); 
val temp2 = 2 * Math.atan2(Math.sqrt(temp1), Math.sqrt(1-temp1)); 
var distance = radius * temp2;
return distance;
}

def calcDensity(lat:Double, lon:Double): Long = {
val clusters = sc.textFile(CLUSTERS_PART).map(_.replace("]","").replace("[","").split(",")).filter(line => getDisplacement(line(1).toDouble, lat, line(0).toDouble, lon) < 0.5)
return clusters.count
}

val resData = sc.textFile(RESTAURANTDATA_PART).map(_.split("\t"))
val clusters = sc.textFile(CLUSTERS_PART)
val resData2 = resData.keyBy(line => line(1))
println("Enter the restaurant name")
//var restaurant = Console.readLine()
var restaurant = "MERCI MARKET"
//40.638431, -74.034668
println("Enter your current Latitude")
//var userLat = Console.readLine().toDouble
var userLat = 40.638431
println("Enter your current Longitude")
//var userLon = Console.readLine().toDouble
var userLon = -74.034668
val details = resData2.lookup(restaurant)
val lat = details.take(1)(0)(3).toDouble
val lon = details.take(1)(0)(4).toDouble
val userDist = getDisplacement(lat, userLat, lon, userLon)
var density = calcDensity(lat,lon)


if(density > 3) {

println("The restaurant you are trying to reach may be busy")
println("Enter the desired restaurant rating")
//var userRating = Console.readLine()
var userRating = "A"
val cuisines = sc.textFile(CUISINEDATA_PART)
val b = cuisines.keyBy(x => x.split("\t")(0))
val c = b.map(line => (line._1, line._2.split("\t").drop(1)))

var cuisine = "American"
val alternate = c.lookup(details.take(1)(0)(6))
println("ithe pochlo")
//check if the lat and lon are correctly placed
val hereYouGo = alternate.flatMap(x => x).filter(x => x.split(",").length == 3).filter(x => getDisplacement(userLat, x.split(",")(1).toDouble,  userLon, x.split(",")(2).toDouble) <= userDist)

val h = hereYouGo.filter(x => calcDensity(x(2).toDouble, x(1).toDouble) < 3)
val hyg = sc.parallelize(h).map(_.split(","))


val a = sc.textFile("/user/abm523/NewYorkTaxi/boilerplate.csv")
val aa = a.map(line => line.split(",")).filter(line => line.length == 7)
val rdd1 = aa.map(line => Row( line(0), line(1), line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble, 0.0, line(6).toDouble))
val rdd2 = hyg.map(x => Row("FRIDAY", "0:00", 40.638431, -74.034668, x(1).toDouble, x(2).toDouble, 0.0, getDisplacement(40.638431, x(1).toDouble,-74.034668, x(2).toDouble)))
val pred = rdd1 ++ rdd2
val schema = new StructType().add(StructField("Day", StringType, true)).add(StructField("Time", StringType, true)).add(StructField("Pick-lat", DoubleType, true)).add(StructField("Pick-lon", DoubleType, true)).add(StructField("Drop-lat", DoubleType, true)).add(StructField("Drop-lon", DoubleType, true)).add(StructField("label", DoubleType, true)).add(StructField("Dist", DoubleType, true))
var dataframe = sqlContext.createDataFrame(pred, schema)
val timeIndexer = new StringIndexer().setInputCol("Time").setOutputCol("TimeIndexed")
val dayIndexer = new StringIndexer().setInputCol("Day").setOutputCol("DayIndexed")
val encoder = new OneHotEncoder().setInputCol("DayIndexed").setOutputCol("DayEncoded")
val encoder2 = new OneHotEncoder().setInputCol("TimeIndexed").setOutputCol("TimeEncoded")
dataframe = timeIndexer.fit(dataframe).transform(dataframe)
dataframe = dayIndexer.fit(dataframe).transform(dataframe)
dataframe = encoder.transform(dataframe)
dataframe = encoder2.transform(dataframe)
var assembler = new VectorAssembler().setInputCols(Array("Dist","Pick-lat", "Pick-lon", "Drop-lon", "Drop-lat", "TimeEncoded", "DayEncoded")).setOutputCol("features") 
dataframe = assembler.transform(dataframe)
dataframe = dataframe.filter("Dist>0")
val m = sc.objectFile[PipelineModel]("/user/abm523/NewYorkTaxi/model").first()
// Make predictions.
val predictions = m.transform(dataframe)

predictions.select("prediction").orderBy($"prediction").show(10)
//import model, predict the fare to the model, take least 10 fares.


val preRDD = predictions.rdd
val preLL = preRDD.map(line => (line(4), line(5)))
val t = hyg.map(line => line(0) + "," + (line(1), line(2))).map(line =>  line.split(","))
//val jhyg = hyg.keyBy(line => line._2)
//val joinhyg = preLL.keyBy(line => line).join(hyg)
println("hereYouGo")

}







































val resLookUp = resData.keyBy(line => line(1))
def getTudes(s:String): String = {
try{
val keyedresData = resLookUp.lookup(s)
print(keyedresData)
val result = s + "," + keyedresData.take(1)(0)(4) + "," + keyedresData.take(1)(0)(5)
return result
}
catch{
case _: Throwable => return "didn't work"
}
}

def temp(s:String):String = {
return s + "!"
}


def getLatLon(s: String): String = {
try{
val res = resData.filter(line => line(1) == s)
return res.take(1)(0)(3) + "," + res.take(1)(0)(4)
} catch {
case _: Throwable => return "didn't work"
}

}

alterBC.map(line => getTudes("GOLDIE'S")).take(1)
alterBC.map(line => getLatLon("GOLDIE'S")).take(1)


//.filter(x => x(7) == userRating)
//val alternateByRating = .filter(x=> getDisplacement(lat, x(lat), lon, x(lon)) < userDist).filter(x => x(7) == userRating)

}

