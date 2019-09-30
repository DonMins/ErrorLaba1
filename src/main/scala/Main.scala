import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Logger,Level}
import org.apache.spark._
import org.apache.spark.storage.StorageLevel

object Main {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache-project").setLevel(Level.WARN)
  def main(args: Array[String]) {
    val cfg = new SparkConf()
      .setAppName("Test").setMaster("local[2]")

    System.setProperty("hadoop.home.dir", "C:/opt/mapr/hadoop/hadoop-2.7.0")
    val sc = new SparkContext(cfg)

    val textFile = sc.textFile("file:///C:/documents/list_of_countries_sorted_gini.txt")
    textFile.foreach(println)



    val tripData = sc.textFile("file:///C:/documents/trips.csv") // запомним заголовок, чтобы затем его исключить
    val tripsHeader = tripData.first
    val trips = tripData.filter(row=>row!=tripsHeader).map(row=>row.split(",",-1)) // считали трипы
    val stationData = sc.textFile("file:///C:/documents/stations.csv")
    val stationsHeader = stationData.first
    val stations = stationData.filter(row => row != stationsHeader).map(row => row.split(",", -1))// считали станции
    System.out.println(stationsHeader)
    System.out.println(tripsHeader)

    stations.take(5).foreach(indvArray => indvArray.foreach(println))
    trips.take(5).foreach(indvArray => indvArray.foreach(println))

    val stationsIndexed = stations.keyBy(row=>row(0).toInt)
    val tripsIndexed = trips.keyBy(row=>row(0).toInt)

    val tripsByStartTerminals = trips.keyBy(row=>row(4).toInt)
    val tripsByEndTerminals = trips.keyBy(row=>row(7).toInt)

    val startTrips =
      stationsIndexed.join(tripsByStartTerminals)
    val endTrips =
      stationsIndexed.join(tripsByEndTerminals)
    println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
    startTrips.toDebugString
    println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh1")
    endTrips.toDebugString

    System.out.println(startTrips.count()+ "countStart")
    System.out.println(endTrips.count()+ "countEnd")



    //    hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh

    case class Station(
                        stationId:Integer,
                        name:String,
                        lat:Double,
                        long:Double,
                        dockcount:Integer,
                        landmark:String,
                        installation:String,
                        notes:String)
    case class Trip(
                     tripId:Integer,
                     duration:Integer,
                     startDate:String,
                     startStation:String,
                     startTerminal:Integer,
                     endDate:String,
                     endStation:String,
                     endTerminal:Integer,
                     bikeId: Integer,
                     subscriptionType: String,
                     zipCode: String)

    val timeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd H:m")
//
    val tripsInternal = trips.map(row=>
      new Trip(tripId=row(0).toInt,
        duration=row(1).toInt,
        startDate= (row(2)),
        startStation=row(3),
        startTerminal=row(4).toInt,
        endDate=(row(5)),
        endStation=row(6),
        endTerminal=row(7).toInt,
        bikeId=row(8).toInt,
        subscriptionType=row(9),
        zipCode=row(10)))
//
//
    System.out.println(tripsInternal.first + "hz")
    System.out.println(tripsInternal.first.startDate + "start date")
//
    val stationsInternal = stations.map(row=>
      new Station(stationId=row(0).toInt,
        name=row(1),
        lat=row(2).toDouble,
        long=row(3).toDouble,
        dockcount=row(4).toInt,
        landmark=row(5),
        installation=row(6),
          notes=null))

    val tripsByStartStation =
      tripsInternal.keyBy(row=>row.startStation)
    val tripsByEndStation =
      tripsInternal.keyBy(row=>row.endStation)

    val start = System.nanoTime()

    val avgDurationByStartStation = tripsByStartStation
      .mapValues(x=>x.duration)
      .groupByKey()
      .mapValues(col=>col.reduce((a,b)=>a+b)/col.size)


    val finish = System.nanoTime()
    val timeConsumedMillis = finish - start

    avgDurationByStartStation.take(10).foreach(println)

    println("Time "+timeConsumedMillis/1000000)

    val start1 = System.nanoTime()

    val avgDurationByStartStation2 = tripsByStartStation
      .mapValues(x=>x.duration)
      .aggregateByKey((0,0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1+acc2._1, acc1._2+acc2._2))
      .mapValues(acc=>acc._1/acc._2)

    val finish1 = System.nanoTime()
    val timeConsumedMillis1 = finish1 - start1

    println("Time1 "+timeConsumedMillis1/100000)
    println("out of the average2")
    avgDurationByStartStation2.take(10).foreach(println)

   ////
//    //1 вариант
   val firstGrouped = tripsByStartStation
     .groupByKey()
     .mapValues(list =>
       list.toList.sortWith((trip1, trip2) => trip1.startDate.compareTo(trip2.startDate)<0))
//    //2 вариант
   val firstGrouped2 = tripsByStartStation
     .reduceByKey((trip1,trip2) =>
       if (trip1.startDate.compareTo(trip2.startDate)<0)
         trip1 else trip2)

   firstGrouped.take(10).foreach(println)
   firstGrouped2.take(10).foreach(println)
//
   val avgDurationByEndStation = tripsByEndStation
     .mapValues(x=>x.duration)
     .aggregateByKey((0,0))(
       (acc, value) => (acc._1 + value, acc._2 + 1),
       (acc1, acc2) => (acc1._1+acc2._1, acc1._2+acc2._2))
     .mapValues(acc=>acc._1/acc._2)
   avgDurationByEndStation.take(10).foreach(println)


//
  avgDurationByStartStation2.collect
  avgDurationByEndStation.collect

   trips.persist(StorageLevel.MEMORY_ONLY)

//    trips.unpersist(true) // другой вариант хранинения
//    trips.persist(StorageLevel.MEMORY_ONLY_SER)


   System.out.println(avgDurationByStartStation2.collect)
   System.out.println(avgDurationByEndStation.collect)

    sc.stop()
  }
}
