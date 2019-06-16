val MIN_LONGITUDE = -74.257159
val MAX_LONGITUDE = -73.699215
val MAX_LATITUDE = 40.915568
val MIN_LATITUDE = 40.495992
val MAX_DISTANCE = 100
val MIN_DISTANCE = 0.5
val MIN_FARE = 2.5
val RAW_FILEPATH = "/user/abm523/taxiMyRestaurant/yellowTaxiData/*"
val CLEAN_FOLDERPATH = "/user/abm523/NewYorkTaxi/finalTaxiData"
val RAWRDD = sc.textFile(RAW_FILEPATH).filter(line => !line.isEmpty)
val HEADER = "vendor"
val rdd1 = RAWRDD.filter(line => !line.toLowerCase.contains(HEADER))
val rdd2 = rdd1.map(line => line.split(",")).filter(line => !line.isEmpty)
val rdd21 = rdd2.filter(line => !line(1).isEmpty)
val rdd22 = rdd21.filter(line => !line(2).isEmpty)
val rdd23 = rdd22.filter(line => !line(3).isEmpty)
val rdd24 = rdd23.filter(line => !line(4).isEmpty)
val rdd25 = rdd24.filter(line => !line(5).isEmpty)
val rdd26 = rdd25.filter(line => !line(6).isEmpty)
val rdd27 = rdd26.filter(line => !line(9).isEmpty)
val rdd28 = rdd27.filter(line => !line(10).isEmpty)
val rdd29 = rdd28.filter(line => !line(15).isEmpty)
val rdd30 = rdd29.filter(line => !line(17).isEmpty)
val rdd3 = rdd30.map(line => (line(1), line(2), line(3), line(4).toDouble * 1.160934, line(5), line(6), line(9), line(10), line(18).toDouble - line(15).toDouble))
val rdd4 = rdd3.filter(line => line._3.toInt > 0)
val rdd5 = rdd4.filter(line => line._4.toDouble > MIN_DISTANCE && line._4.toDouble < MAX_DISTANCE)
val rdd6 = rdd5.filter(line => line._5.toDouble > MIN_LONGITUDE && line._5.toDouble < MAX_LONGITUDE)
val rdd7 = rdd6.filter(line => line._6.toDouble > MIN_LATITUDE && line._6.toDouble < MAX_LATITUDE)
val rdd8 = rdd7.filter(line => line._7.toDouble > MIN_LONGITUDE && line._7.toDouble < MAX_LONGITUDE)
val rdd9 = rdd8.filter(line => line._8.toDouble > MIN_LATITUDE && line._8.toDouble < MAX_LATITUDE)
val rdd10 = rdd9.filter(line => line._1 != line._2);
val rdd11 = rdd10.filter(line => !(line._5 == line._7 && line._6 == line._8));
val rdd12 = rdd11.filter(line => line._9.toDouble >= MIN_FARE);
val finalData = rdd12.map(line => line._1 + "," + line._2 + "," + line._3 + "," + line._4 + "," + line._5 + "," + line._6 + "," + line._7 + "," + line._8 + "," + line._9)

finalData.saveAsTextFile(CLEAN_FOLDERPATH);