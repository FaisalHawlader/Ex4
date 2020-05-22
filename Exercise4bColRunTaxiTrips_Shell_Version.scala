sh ~/Downloads/spark-2.3.1-bin-hadoop2.7/bin/spark-shell 

val df_raw = spark.read.format("csv").option("header", "true").load("Data/Motor_Vehicle_Collisions.csv")
val df_raw_s = df_raw.select("CRASH DATE","CRASH TIME","BOROUGH","LATITUDE","LONGITUDE","LOCATION","ON STREET NAME","CROSS STREET NAME","OFF STREET NAME","NUMBER OF PERSONS INJURED","NUMBER OF PERSONS KILLED","CONTRIBUTING FACTOR VEHICLE 1","CONTRIBUTING FACTOR VEHICLE 2","VEHICLE TYPE CODE 1","VEHICLE TYPE CODE 2")


//FILTER OUT USELESS OR MEANINGLESS LINES 
val df_good = df_raw_s.
filter(col("LATITUDE").isNotNull).
filter(col("LONGITUDE").isNotNull).
filter(col("ON STREET NAME").isNotNull).
filter(col("CROSS STREET NAME").isNotNull).
filter(!$"CRASH DATE".contains("PARK"))



println("\n\n\n Problem 2 (a) ")
println("Intital no of rows in Rdd "+df_raw_s.count)
println("No of rows in Rdd  after removing Null Co-ordinates/Null ON STREET NAME or CROSS STREET NAME : "+df_good.count)
println("Some random rows from dataset after parsing : ")
df_good.show(5)


//FIND TOP DANGEROUS STREET AND IT'S CONTRIBUTING FACTORS

val dangerousStreeDf = df_good.select("ON STREET NAME","CROSS STREET NAME","OFF STREET NAME","NUMBER OF PERSONS INJURED","NUMBER OF PERSONS KILLED","CONTRIBUTING FACTOR VEHICLE 1","CONTRIBUTING FACTOR VEHICLE 2").withColumn("PERSONS AFFECTED",$"NUMBER OF PERSONS KILLED"+$"NUMBER OF PERSONS INJURED")

val dangerousStreeDf_sum = dangerousStreeDf.groupBy("ON STREET NAME","CROSS STREET NAME").agg(sum("PERSONS AFFECTED").as("TOTAL PERSONS AFFECTED")).sort(desc("TOTAL PERSONS AFFECTED"))
val top5Street = dangerousStreeDf_sum.limit(5)

println("\n\n\n Problem 2 (b) ")
println("Top 5 most dangerous street crossings based on no of persons injured or killed : ")
top5Street.show(false)


val cf1 = df_good.select("ON STREET NAME","CROSS STREET NAME","CONTRIBUTING FACTOR VEHICLE 1").withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 1", "CONTRIBUTING FACTOR")
val cf2 = df_good.select("ON STREET NAME","CROSS STREET NAME","CONTRIBUTING FACTOR VEHICLE 2").withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 2", "CONTRIBUTING FACTOR")

val cfUnionDf = cf1.union(cf2).filter(!$"CONTRIBUTING FACTOR".contains("Unspecified")).filter($"CONTRIBUTING FACTOR".isNotNull)

val top10DanJoinedCf = cfUnionDf.join(top10Street,Seq("ON STREET NAME","CROSS STREET NAME"))

val countByCfCrossingDf = top10DanJoinedCf.groupBy("ON STREET NAME","CROSS STREET NAME","CONTRIBUTING FACTOR").count

import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window

val w = Window.partitionBy($"ON STREET NAME",$"CROSS STREET NAME").orderBy($"count".desc)

val dfTopCfperStreet = countByCfCrossingDf.withColumn("Rank", row_number.over(w)).where($"Rank" < 11)
println("Top 10 Contributin Factors for each of these top 5 pair of Crossings : ")
dfTopCfperStreet.show(50,false)



//FIND TOP DANGEROUS TYPE OF CAR
val top10CF = dfTopCfperStreet.groupBy("CONTRIBUTING FACTOR").agg(sum("count").as("count")).sort(desc("count")).limit(10)

val cfWithVehicle1 = df_good.select("CONTRIBUTING FACTOR VEHICLE 1","VEHICLE TYPE CODE 1").toDF("CONTRIBUTING FACTOR","VEHICLE TYPE CODE")
val cfWithVehicle2 = df_good.select("CONTRIBUTING FACTOR VEHICLE 2","VEHICLE TYPE CODE 2").toDF("CONTRIBUTING FACTOR","VEHICLE TYPE CODE")

val cfWithVehicleSportPass = cfWithVehicle1.union(cfWithVehicle2).filter(!$"CONTRIBUTING FACTOR".contains("Unspecified")).filter($"CONTRIBUTING FACTOR".isNotNull).
filter($"VEHICLE TYPE CODE".contains("PASSENGER VEHICLE") || $"VEHICLE TYPE CODE".contains("SPORT UTILITY"))


val topCfWithVehicle = cfWithVehicleSportPass.join(top10CF,Seq("CONTRIBUTING FACTOR"))

val countByVehicleType = topCfWithVehicle.groupBy("VEHICLE TYPE CODE").count

println("\n\n\n Problem 2 (c) ")
println("Based on problem 2 b, We take top CONTRIBUTING FACTORS")
println("Based on these top 10 CONTRIBUTING FACTORS , we find Count of the Type Of vehicle associated with these factors ")
println("Counting the Occurance of vehicle type of Sports Utility and Passenger : ")
countByVehicleType.show(false)
println("Clearly PASSENGER VEHICLE is more dangerous ")


//-----------PROBLEM 4 D-------


//PREPARE CRASH DATA
val crashDF_raw = df_good.select("CRASH DATE","CRASH TIME","LATITUDE","LONGITUDE","CONTRIBUTING FACTOR VEHICLE 1","CONTRIBUTING FACTOR VEHICLE 2")

val createDateStringUDF = udf { (date: String, time: String) => {
	val dateArr = date.split("/")
	dateArr(2)+"-"+dateArr(0)+"-"+dateArr(1)+" "+time+":00"} 
	}

val crashDF = crashDF_raw.withColumn("CRAASH_DATETIME",createDateStringUDF(col("CRASH DATE"),col("CRASH TIME"))).drop("CRASH DATE","CRASH TIME")

//PREPARE TAXI DATA

    def parse(line: String): (String,Double,Double,String,Double,Double) = {
      val fields = line.split(',')
      val pickupTime = fields(5)
      val dropoffTime = fields(6)
      val pickupLat = fields(10).toDouble
      val pickupLong = fields(11).toDouble
      val dropoffLat = fields(12).toDouble
      val dropoffLong = fields(13).toDouble
      (pickupTime, pickupLat, pickupLong, dropoffTime, dropoffLat, dropoffLong)
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
    val taxiRaw = sc.textFile("Data" + "/nyc-taxi-trips").sample(false, 0.01) // use 1 percent sample size for debugging!

    // STEP - CLEAN DATA
    val taxiParsed = taxiRaw.map(safe(parse))

    val taxiGood = taxiParsed.collect({
      case t if t.isLeft => t.left.get
    })
    taxiGood.cache() // cache good lines for later re-use
   val taxiDF = taxiGood.toDF("PICKUP_TIME", "PICKUP_LAT", "PICKUP_LONG", "DROPOFF_TIME", "DROPOFF_LAT", "DROPOFF_LONG")



//COMBINE EACH TAXITRIP WITH CRASH EVENT
spark.conf.set("spark.sql.crossJoin.enabled","true")
val taxiCrashDF = crashDF.join(taxiDF)


// Returns Distance between 2 Co-ordinates
def distance_latlong (lat1 : Double, lon1 : Double, lat2 : Double, lon2 : Double) : Int = {
	val AVERAGE_RADIUS_OF_EARTH_KM : Int = 6371;

	 val latDistance = Math.toRadians(lat1 -lat2)
        val lngDistance = Math.toRadians(lon1 - lon2)
        val sinLat = Math.sin(latDistance / 2)
        val sinLng = Math.sin(lngDistance / 2)
        val a = sinLat * sinLat +
        (Math.cos(Math.toRadians(lat1)) *
            Math.cos(Math.toRadians(lat2)) *
            sinLng * sinLng)
        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      return  (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
}


// Returns Distance between the Crash Co-ordinate and The Line joining taxi Trips in meters
  def pDistance(x : Double, y: Double, x1: Double, y1: Double, x2: Double, y2: Double) : Double =  {

    val A = x - x1;
    val B = y - y1;
    val C = x2 - x1;
    val D = y2 - y1;

    val dot = A * C + B * D;
    val len_sq = C * C + D * D;
    var param : Double = -1;
    if (len_sq != 0) //in case of 0 length line
        param = dot / len_sq;

    var xx : Double = 0;
    var yy : Double= 0;

    if (param < 0) {
      xx = x1;
      yy = y1;
    }
    else if (param > 1) {
      xx = x2;
      yy = y2;
    }
    else {
      xx = x1 + param * C;
      yy = y1 + param * D;
    }

  return  distance_latlong(xx, yy, x, y)

  }
 

def checkIfDistLessThan50(x : String, y: String, x1: String, y1: String, x2: String, y2: String) : Boolean = {
	if(pDistance(x.toDouble,y.toDouble,x1.toDouble,y1.toDouble,x2.toDouble,y2.toDouble) <= 50){
		return true
	}
	else{
		return false
	}
}
val checkIfDistLessThan50UDF = udf(checkIfDistLessThan50 _)

import java.text.SimpleDateFormat

val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
def checkifDateValid(pickupTime : String, dropoffTime : String, crashTime :String) : Boolean = {

	val pickupTimeJ = new org.joda.time.DateTime(formatter.parse(pickupTime))
	val dropoffTimeJ = new org.joda.time.DateTime(formatter.parse(dropoffTime))
	val crashTimeJ = new org.joda.time.DateTime(formatter.parse(crashTime))

	if(pickupTimeJ.isBefore(crashTimeJ) && crashTimeJ.isBefore(dropoffTimeJ))
		return true;
	else
		return false;
}
val checkifDateValidUDF = udf(checkifDateValid _)

//FILTER TO CHECK IF CRASH TIME BETWEEN PICKUP_TIME and DROPOFF_TIME
val dfTaxiCrashDateValid = taxiCrashDF.filter(checkifDateValidUDF(col("PICKUP_TIME"),col("DROPOFF_TIME"),col("CRAASH_DATETIME")))

//FILTER TO CHECK IF CRASH POINT NEAR TO 50M FROM LINE JOINING PICKUP_LOC and DROPOFF_LOC
val dfTaxiCrashDateLocationValid = dfTaxiCrashDateValid.filter(checkIfDistLessThan50UDF(col("LATITUDE"),col("LONGITUDE"),col("PICKUP_LAT"),col("PICKUP_LONG"),col("DROPOFF_LAT"),col("DROPOFF_LONG")))

dfTaxiCrashDateLocationValid.select("PICKUP_TIME","PICKUP_LAT","PICKUP_LONG","DROPOFF_TIME","DROPOFF_LAT","DROPOFF_LONG","CONTRIBUTING FACTOR VEHICLE 1","CONTRIBUTING FACTOR VEHICLE 2").show()



println("\n\n\n Problem 2 (D) ")
println("Steps to compute Taxi Trips Collisions : ")
println("      1. Prepare Crash Data : (crash datetime , crash location, crash Contributing Factors)  ")
println("      2. Prepare Taxi Data : (Trip start and end datetime , Trip start and end  location)  ")
println("      3. Join every Taxi data event with  Every Crash Data event ")
println("      4. Select all such event(taxi trip,crash) if Crash_DateTime is between TaxiTrip Duration")
println("      5. Select all such event(taxi trip,crash) if Crash_Location is less than 50m from Taxi Trip's Linear Trajectory")
println("      6. The selected Data are the taxitrips that had met with an collision")
println("Based on these Steps Some random taxi trips that met with an collision with their Contributing factors are : ")

dfTaxiCrashDateLocationValid.select("PICKUP_TIME","PICKUP_LAT","PICKUP_LONG","DROPOFF_TIME","DROPOFF_LAT","DROPOFF_LONG","CONTRIBUTING FACTOR VEHICLE 1","CONTRIBUTING FACTOR VEHICLE 2").show()

