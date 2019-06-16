def degToRad(degree: Double): Double = {
return degree * (Math.PI/180)
}

def getManDistance(lat1: Double, lat2: Double, lon1: Double, lon2: Double): Double ={
val radius = 6371;
val disLat = degToRad(lat2-lat1);
val disLon = degToRad(lon2-lon1);
var a = Math.sin(disLat/2) * Math.sin(disLat/2)
var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
val latitudeDistance = radius * c
a =  Math.sin(disLon/2) * Math.sin(disLon/2);
c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
val longitudeDistance = radius * c
return Math.abs(latitudeDistance) + Math.abs(longitudeDistance)

}

def getTime(t:String): String = {
    val hour = t.split(":")(0).toInt
    var mins = t.split(":")(1).toInt
    var converted = (mins/15) * 15
    val time: String = hour+":"+converted
    return time
}

def getTimeDiff(t1:String, t2:String): Double ={
val formatter_datetime = new java.text.SimpleDateFormat("HH:mm:ss")
val parsedt1 = formatter_datetime.parse(t1)
val parsedt2 = formatter_datetime.parse(t2)
val date1 = new java.sql.Date(parsedt1.getTime())
val date2 = new java.sql.Date(parsedt2.getTime())
val duration = date2.getTime() - date1.getTime()

return (duration.toDouble/3600000)
}


def getSpeed(d:Double, t:Double):Double ={
return d/t
}


val rdd = sc.textFile("/user/sjd451/project/cleanTaxiData")
// val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty).map(line => getSpeed(line(3).toDouble, getTimeDiff(line(0).split(" ")(1), line(1).split(" ")(1))) +","+ line(3) + "," + getTimeDiff(line(0).split(" ")(1), line(1).split(" ")(1)))

val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
val avgSpeedOfTrip = rdd1.map(line => ( getTime(line(0).split(" ")(1)), getSpeed(line(3).toDouble, getTimeDiff(line(0).split(" ")(1), line(1).split(" ")(1)))))
val avgSpeed = avgSpeedOfTrip.reduceByKey(_+_)
val rdd2 = rdd1.map(x => (getTime(x(0).split(" ")(1)), 1))
val rideCount = rdd2.reduceByKey(_+_)
var avgSpeedPerTime = avgSpeed.join(rideCount).sortByKey().map(x => x._1 + "," + x._2._1.toDouble/x._2._2.toDouble + "," + x._2._2.toDouble)
avgSpeedPerTime.saveAsTextFile("/user/sjd451/project/g3")
