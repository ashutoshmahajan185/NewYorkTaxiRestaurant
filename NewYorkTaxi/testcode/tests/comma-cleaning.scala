 def createRestaurantDataRDD(sc: org.apache.spark.SparkContext): Unit = {
        val path = "hdfs:///user/sjd451/project/restaurant.csv"

        val rawrdd = sc.textFile(path)

        val header =rawrdd.first()

        val rdd =rawrdd.filter(line => line!=header).map(line => line.split(","))

        val rdd1 = rdd.map(line => makeProper(line))

        //TODO: covert the address to lat-long using th above function and store it as a value
        val rdd2 = rdd1.keyBy(line => line(0))

        val rdd3 = rdd2.reduceByKey((v1,v2) => v1)
    }

  def makeProper(arr: Array[String]): Array[String] ={
          val len = arr.length
          var lineStr = ""
          if (len == 18){
              lineStr =  arr(0)+","+arr(1)+","+arr(3)+" "+arr(4)+" "+arr(2)+" "+arr(5)+","+arr(6)+","+arr(7)+","+arr(13)+","+arr(14)
          }
          else {
              var a = 0
              var cuisine = ""
              for(a <- 7 to len-10){
                  cuisine = cuisine+arr(a)+" k "
              }
              lineStr =  arr(0)+","+arr(1)+","+arr(3)+" "+arr(4)+" "+arr(2)+" "+arr(5)+","+arr(6)+","+cuisine+","+arr(len-5)+","+arr(len-4)
          }
          return lineStr.split(",")

  }