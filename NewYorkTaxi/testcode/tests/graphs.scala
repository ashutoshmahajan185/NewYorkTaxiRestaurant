def getTime(t:String): String = {
    val hour = t.split(":")(0).toInt
    var mins = t.split(":")(1).toInt
    var converted = (mins/15) * 15
    val time: String = hour+":"+converted
    return time
}

val rdd = sc.textFile("/user/sjd451/project/cleanTaxiData")
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
val rideFare = rdd1.map(x => (getTime(x(0).split(" ")(1)), x(8).substring(0, x(8).length-1).toDouble))
val totalFare = rideFare.reduceByKey(_+_)
val rdd2 = rdd1.map(x => (getTime(x(0).split(" ")(1)), 1))
val rideCount = rdd2.reduceByKey(_+_)
val avgFare = totalFare.join(rideCount).map(x => x._1 + "," + x._2._1.toDouble/x._2._2.toDouble)
avgFare.saveAsTextFile("/user/sjd451/project/graphs")

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
val rdd = sc.textFile("/user/sjd451/project/cleanTaxiData")
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
val rideFare = rdd1.map(x => (getDay(x(0).split(" ")(0)), x(8).substring(0, x(8).length-1).toDouble))
val totalFare = rideFare.reduceByKey(_+_)
val rdd2 = rdd1.map(x => (getDay(x(0).split(" ")(0)), 1))
val rideCount = rdd2.reduceByKey(_+_)
val avgFare = totalFare.join(rideCount).map(x => x._1 + "," + x._2._1.toDouble/x._2._2.toDouble + "," + x._2._2)
avgFare.saveAsTextFile("/user/sjd451/project/g2")
//time, fare, speed, count