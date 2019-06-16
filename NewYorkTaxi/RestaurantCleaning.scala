import java.util.Random

val FILEPATH = "/user/abm523/taxiMyRestaurant/restaurantData/RestaurantData.tsv"
val GEOPATH = "/user/abm523/taxiMyRestaurant/restaurantData/resLatLon.txt"
val RESTAURANTDATA = "/user/abm523/NewYorkTaxi/restaurantData"
val CUISINEDATA = "/user/abm523/NewYorkTaxi/cuisineData"
val RAWRDD = sc.textFile(FILEPATH)
val LATLONRDD = sc.textFile(GEOPATH)

def giveGrade(grade: String): String = {}
	val random = new Random()
	// grade is empty
	if(grade == "") {
		val r = random.nextInt(100)
		if(r >= 0 && r <= 33) {
			return "A"
		}
		if(r >= 34 && r <= 66) {
			return "B"
		}
		if(r >= 67 && r <= 100) {
			return "C"
		}
	}
	return g
}

def formatCuisine(cuisine: String): String = {
	if(cuisine.contains(",")) {
		cuisine.replace(", ", "/")
	}
	if(cuisine.contains("Latin")) {
		return "Latin"
	}
	return cuisine
}

val header = RAWRDD.first()
val rdd = RAWRDD.filter(line => line != header).map(line => line.split("\t"))
val filteredRDD = rdd.filter(line => line.length == 18)

val rdd1 = filteredRDD.map(arr => ((arr(0), arr(1), arr(3)+" "+arr(4)+" "+arr(2)+" "+arr(5), arr(6), arr(7), giveGrade(arr(14)))))
val rdd11 = rdd1.filter(line => !line._4.isEmpty)
val rdd12 = rdd11.filter(line => !line._5.isEmpty)

val rdd2 = rdd12.keyBy(line => line._1)
val rdd3 = rdd2.reduceByKey((v1,v2) => v1)
val finalrdd = rdd3.map(line => line._2)

// Joining geocoded data
val llrdd = LATLONRDD.filter(line => !line.isEmpty).map(line => line.split(","))
val llrddkey = llrdd.map(line => (line(0), (line(1), line(2)))).distinct
val resrddkey = finalrdd.map(line => (line._3, (line._1, line._2, line._4, line._5, line._6))).distinct

val joinedResData = llrddkey.join(resrddkey)
val finalResData = joinedResData.map(line => line._2._2._1.replace("(","").trim + "\t" + line._2._2._2 + "\t" + line._1 + "\t" + line._2._1._1 + "\t" + line._2._1._2 + "\t" + line._2._2._3 + "\t" + formatCuisine(line._2._2._4) + "\t" + line._2._2._5.replace(")","").trim)

// used for geocoding. Not required while running application. Comment after first run to save unnecessary processing
val geoCode = finalrdd.map(line => line._3).distinct()
geoCode.saveAsTextFile("taxiMyRestaurant/geoCode")
finalResData.saveAsTextFile(RESTAURANTDATA)
// used for geocoding. Not required while running application. Comment after first run to save unnecessary processing

//Combining the restaurants by their cuisines and saving them
val rdd5 = finalResData.map(line => line.split("\t")).map(fields => (fields(6), fields(1) + "," + fields(3) + "," + fields(4)))
val rdd6 = rdd5.reduceByKey((v1, v2) => v1 + "\t" + v2)
val cuisines = rdd6.map(line => line._1 + "\t" + line._2)
cuisines.saveAsTextFile(CUISINEDATA)




