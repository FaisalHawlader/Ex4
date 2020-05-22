

object Exercise4RunTaxiTrips {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val dataDirectory = args(0)
    val sampleSize = args(1).toDouble

    val conf = new SparkConf()
      .setMaster("local")
      .set("spark.ui.showConsoleProgress", "false")
      .setAppName("RunTaxiTrips")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    def point(longitude: String, latitude: String): Point = {
      new Point(longitude.toDouble, latitude.toDouble)
    }

    case class TaxiTrip(
      pickupTime:  org.joda.time.DateTime,
      dropoffTime: org.joda.time.DateTime,
      pickupLoc:   com.esri.core.geometry.Point,
      dropoffLoc:  com.esri.core.geometry.Point)

    def parse(line: String): (String, TaxiTrip) = {
      val fields = line.split(',')
      val license = fields(1)
      val pickupTime = new org.joda.time.DateTime(formatter.parse(fields(5)))
      val dropoffTime = new org.joda.time.DateTime(formatter.parse(fields(6)))
      val pickupLoc = point(fields(10), fields(11))
      val dropoffLoc = point(fields(12), fields(13))
      val trip = TaxiTrip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
      (license, trip)
    }

    def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
      new Function[S, Either[T, (S, Exception)]] with Serializable {
        def apply(s: S): Either[T, (S, Exception)] = {
          try {
            Left(f(s))
          } catch {
            case e: Exception => Right((s, e))
          }
        }
      }
    }

    // STEP - READ DATA
    val taxiRaw = sc.textFile(dataDirectory + "/nyc-taxi-trips").sample(false, sampleSize) // use 1 percent sample size for debugging!

    // STEP - CLEAN DATA
    val taxiParsed = taxiRaw.map(safe(parse))
    taxiParsed.cache()

    val taxiGood = taxiParsed.collect({
      case t if t.isLeft => t.left.get
    })
    taxiGood.cache() // cache good lines for later re-use

    println("\n" + taxiGood.count() + " taxi trips parsed.")

    // STEP - READ BOROUGHS
    //----------------- Parse the NYC Boroughs Polygons -----------------------
    import GeoJsonProtocol._
    val geojson = scala.io.Source.fromFile(dataDirectory + "/nyc-borough-boundaries-polygon.geojson").mkString
    val features = geojson.parseJson.convertTo[FeatureCollection]

    val bFeatures = sc.broadcast(features)

    def borough__pick_pday_drop_dday(trip: TaxiTrip): (String, String, String, String) = {
      val d_feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(trip.dropoffLoc)
      })
      val d_loc: String = if (d_feature.isEmpty) {
        "UNKNOWN"
      } else {
        d_feature.get.apply("borough").toString().replaceAll("\"", "")
      }

      val d_day = trip.dropoffTime.dayOfWeek().getAsShortText

      val p_feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(trip.pickupLoc)
      })

      val p_loc: String = if (p_feature.isEmpty) {
        "UNKNOWN"
      } else {
        p_feature.get.apply("borough").toString().replaceAll("\"", "")
      }

      val p_day = trip.pickupTime.dayOfWeek().getAsShortText

      (p_loc, p_day, d_loc, d_day)
    }

    val bcountDf = taxiGood.values.map(borough__pick_pday_drop_dday).toDF("START_BOROUGH", "START_DAY", "END_BOROUGH", "END_DAY")

    println("\n\n\nPROBLEM 1 > a > i : ")
    println("The number of taxi trips which STARTED in each of the NYC boroughs over the entire period of time :  ")
    bcountDf.groupBy("START_BOROUGH").count().sort("START_BOROUGH").show

    println("\n\n\nPROBLEM 1 > a > ii : ")
    println("The number of taxi trips which ENDED in each of the NYC boroughs over the entire period of time :  ")
    bcountDf.groupBy("END_BOROUGH").count().sort("END_BOROUGH").show

    println("\n\n\nPROBLEM 1 > b > i : ")
    println("The number of taxi trips which ENDED in each of the NYC boroughs PER DAY OF WEEK over the entire period of time :  ")
    bcountDf.groupBy("START_BOROUGH", "START_DAY").count().sort("START_BOROUGH", "START_DAY").show(500)

    println("\n\n\nPROBLEM 1 > b > ii : ")
    println("The number of taxi trips which ENDED in each of the NYC boroughs PER DAY OF WEEK over the entire period of time :  ")
    bcountDf.groupBy("END_BOROUGH", "END_DAY").count().sort("END_BOROUGH", "END_DAY").show(500)




    class FirstKeyPartitioner[K1, K2](partitions: Int) extends org.apache.spark.Partitioner {
      val delegate = new org.apache.spark.HashPartitioner(partitions)
      override def numPartitions: Int = delegate.numPartitions
      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[(K1, K2)]
        delegate.getPartition(k._1)
      }
    }

    def secondaryKey(trip: TaxiTrip) = trip.pickupTime.getMillis

    def split(t1: TaxiTrip, t2: TaxiTrip): Boolean = {
      val p1 = t1.pickupTime
      val p2 = t2.pickupTime
      val d = new Duration(p1, p2)
      d.getStandardHours >= 4
    }

    def groupSorted[K, V, S](
      it:        Iterator[((K, S), V)],
      splitFunc: (V, V) => Boolean): Iterator[(K, List[V])] = {
      val res = List[(K, ArrayBuffer[V])]()
      it.foldLeft(res)((list, next) => list match {
        case Nil =>
          val ((lic, _), trip) = next
          List((lic, ArrayBuffer(trip)))
        case cur :: rest =>
          val (curLic, trips) = cur
          val ((lic, _), trip) = next
          if (!lic.equals(curLic) || splitFunc(trips.last, trip)) {
            (lic, ArrayBuffer(trip)) :: list
          } else {
            trips.append(trip)
            list
          }
      }).map { case (lic, buf) => (lic, buf.toList) }.iterator
    }

    def groupByKeyAndSortValues[K: Ordering: ClassTag, V: ClassTag, S: Ordering](
      rdd:              RDD[(K, V)],
      secondaryKeyFunc: V => S,
      splitFunc:        (V, V) => Boolean,
      numPartitions:    Int): RDD[(K, List[V])] = {
      val presess = rdd.map {
        case (lic, trip) => ((lic, secondaryKeyFunc(trip)), trip)
      }
      val partitioner = new FirstKeyPartitioner[K, S](numPartitions)
      presess.repartitionAndSortWithinPartitions(partitioner).mapPartitions(groupSorted(_, splitFunc))
    }

    val sessions = groupByKeyAndSortValues(taxiGood, secondaryKey, split, 30) // use fixed amount of 30 partitions
    sessions.cache()

    def borough(trip: TaxiTrip): Option[String] = {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(trip.dropoffLoc)
      })
      feature.map(f => {
        f("borough").convertTo[String]
      })
    }

    def boroughDuration(t1: TaxiTrip, t2: TaxiTrip) = {
      val b = borough(t1)
      val d = new Duration(t1.dropoffTime, t2.pickupTime)

      (b, d.getStandardHours, t2.pickupTime.hourOfDay().get)
    }

    val boroughDurations: RDD[(Option[String], Long, Int)] =
      sessions.values.flatMap(trips => {
        val iter: Iterator[Seq[TaxiTrip]] = trips.sliding(2)
        val viter = iter.filter(_.size == 2)
        viter.map(p => boroughDuration(p(0), p(1)))
      }).cache()

    val boroughDurationsDf = boroughDurations.toDF("BOROUGH","WAIT_TIME_Hrs", "Hour Of the Day")

    println("\n\n\nPROBLEM 1 > c  : ")
    println("Average duration between two subsequent trips conducted by the same taxi driver per borough and per hour-of-the-day :  ")

    boroughDurationsDf.
    select("BOROUGH","Hour Of the Day","WAIT_TIME_Hrs").
    filter(col("BOROUGH").isNotNull).
    groupBy("BOROUGH","Hour Of the Day").avg("WAIT_TIME_Hrs").
    withColumn("AVG_WAIT_TIME_Hrs", round(col("avg(WAIT_TIME_Hrs)") * 100 / 5) * 5 / 100).drop("avg(WAIT_TIME_Hrs)").
    sort("BOROUGH","Hour Of the Day").show(1000)

        

    def tripDuration_min(t1: TaxiTrip) = {
      val d = new Duration(t1.pickupTime, t1.dropoffTime)
      (t1, d.getStandardMinutes)
    }

    val tripDurationRDD: RDD[(TaxiTrip, Long)] = taxiGood.values.map(tripDuration_min)
    val durationValues = tripDurationRDD.values.collect().sorted
    val index = durationValues.length * 0.95;
    val quantile95value: Double = durationValues.apply(Math.round(index.toFloat))
    println("\n\n\nPROBLEM 1 > d  : ")
    println("The 95% quantile mark for the trip duration is :  " + quantile95value + " mins")
    println("Any taxi trip longer than " + quantile95value + " mins can be a potential outlier")
    println("Some random 5 potential outliers in the given dataset : ")
    tripDurationRDD.map(f => (f._1.toString(), f._2)).toDF("TRIP", "Duration_minutes")
      .filter(col("Duration_minutes") > quantile95value)
      .limit(5)
      .show(false)

    def tripDuration_min_normalized(t1: TaxiTrip) = {
      val dur = new Duration(t1.pickupTime, t1.dropoffTime)
      val dist = GeometryEngine.distance(t1.dropoffLoc, t1.pickupLoc, null)

      val durToDist = dur.getStandardMinutes / dist

      (t1, durToDist)
    }

    val tripDurationDistRDD = taxiGood.values.map(tripDuration_min_normalized)

    val durationDistRatioValues = tripDurationDistRDD.values.collect().sorted
    val index2 = durationDistRatioValues.length * 0.95;
    val quantile95value_normalized: Double = durationDistRatioValues.apply(Math.round(index2.toFloat))
    println("\n\n\nPROBLEM 1 > e  : ")
    println("The 95% quantile mark /ratio for the trips  DURATION to DISTANCE ratio is :  " + quantile95value_normalized)
    println("Any taxi trip having Duration : Distance larger than " + quantile95value_normalized + "  can be a potential outlier")
    println("Some random 5 potential outliers with respect to Duration : Distance ratio in the given dataset : ")

    tripDurationDistRDD.map(f => (f._1.toString(), f._2)).toDF("TRIP", "Duration_to_Distance_ratio")
      .filter(col("Duration_to_Distance_ratio") > quantile95value_normalized)
      .withColumn("Duration_to_Distance_ratio", round(col("Duration_to_Distance_ratio") * 100 / 5) * 5 / 100)
      .limit(5)
      .show(false)

  }
}
