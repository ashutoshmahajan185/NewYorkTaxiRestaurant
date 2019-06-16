 def createRestaurantData(sc: org.apache.spark.SparkContext): Unit = {
        val path = "hdfs:///user/sjd451/project/res.tsv"

        val rawrdd = sc.textFile(path)

        val header =rawrdd.first()
        //removing the header of the file and reading the tab-seperated-value file
        val rdd =rawrdd.filter(line => line!=header).map(line => line.split("\t"))
        //filter all data with missing values and corrupt data
        val filteredRDD = rdd.filter(line => line.length == 18)
        //Extract restaurant information
        val rdd1 = filteredRDD.map(arr => (arr(0)+"\t"+arr(1)+"\t"+arr(3)+" "+arr(4)+" "+arr(2)+" "+arr(5)+"\t"+arr(6)+"\t"+arr(7)+"\t"+arr(13)+"\t"+arr(14)).split("\t"))

        //TODO: covert the address to lat-long using th above function and store it as a value

        //Combining multiple values for the same restaurant and saving them
        val rdd2 = rdd1.keyBy(line => line(0))

        val rdd3 = rdd2.reduceByKey((v1,v2) => v1)

        val rdd4 = rdd3.map(line => line._2)

        rdd4.saveAsTextFile("project/restaurant-info")

        //Combining the restaurants by their cuisines and saving them
        val rdd5 = rdd4.map(fields => (fields(4), fields(1)))

        val rdd6 = rdd5.reduceByKey((v1,v2) => v1+","+v2)

        rdd6.saveAsTextFile("project/cuisine")

    }