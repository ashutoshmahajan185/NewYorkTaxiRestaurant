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
    //to remove "(" at the start"
    
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

val CLEAN_FOLDERPATH = "/user/abm523/NewYorkTaxi/finalTaxiData/*"
val GRAPHSPATH1 = "/user/abm523/NewYorkTaxi/graph1"
val GRAPHSPATH2 = "/user/abm523/NewYorkTaxi/graph2"

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

val rdd = sc.textFile(CLEAN_FOLDERPATH)
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
//graphs based on time of day
val rdd2 = rdd1.map(x => (getTime(x(0).split(" ")(1)), 1))
val rideCount = rdd2.reduceByKey(_+_)
//time v avgFare
val rideFare = rdd1.map(x => (getTime(x(0).split(" ")(1)), x(8).toDouble))
val totalFare = rideFare.reduceByKey(_+_)
//time v speed
val avgSpeedOfTrip = rdd1.map(line => ( getTime(line(0).split(" ")(1)), getSpeed(line(3).toDouble, getTimeDiff(line(0).split(" ")(1), line(1).split(" ")(1)))))
val avgSpeed = avgSpeedOfTrip.reduceByKey(_+_)
val graph = rideCount.join(totalFare).join(avgSpeed).map(x => x._1 +","+ x._2._1._2.toDouble/x._2._1._1.toDouble +","+ x._2._2.toDouble/x._2._1._1.toDouble +","+ x._2._1._1)
//(String, ((Int, Double), Double))
graph.saveAsTextFile(GRAPHSPATH1)
//graphs based on days of the week
val rdd2 = rdd1.map(x => (getDay(x(0).split(" ")(0)), 1))
val rideCount = rdd2.reduceByKey(_+_)
//day v avgFare & #trips
val rideFare = rdd1.map(x => (getDay(x(0).split(" ")(0)), x(8).toDouble))
val totalFare = rideFare.reduceByKey(_+_)
val avgFare = totalFare.join(rideCount).map(x => x._1 + "," + x._2._1.toDouble/x._2._2.toDouble + "," + x._2._2)
avgFare.saveAsTextFile(GRAPHSPATH2)
