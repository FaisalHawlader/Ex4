// spark-shell 


val dataDirectory = "Data"

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

// reads a stock history in the Google time-series format
    def readStockHistory(file: File): Array[(LocalDate, Double)] = {
      val formatter = DateTimeFormatter.ofPattern("d-MMM-yy")
      val lines = scala.io.Source.fromFile(file).getLines().toSeq
      lines.tail.map { line =>
        val cols = line.split(',')
        val date = LocalDate.parse(cols(0), formatter)
        val value = cols(4).toDouble
        (date, value)
      }.reverse.toArray
    }

   // keep only values within given start- and end-date
    def trimToRegion(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate): Array[(LocalDate, Double)] = {
      var trimmed = history.dropWhile(_._1.isBefore(start)).
        takeWhile(x => x._1.isBefore(end) || x._1.isEqual(end))
      if (trimmed.head._1 != start) {
        trimmed = Array((start, trimmed.head._2)) ++ trimmed
      }
      if (trimmed.last._1 != end) {
        trimmed = trimmed ++ Array((end, trimmed.last._2))
      }
      trimmed
    }

    import scala.collection.mutable.ArrayBuffer

    // impute missing values of trading days by their last known value
    def fillInHistory(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate): Array[(LocalDate, Double)] = {
      var cur = history
      val filled = new ArrayBuffer[(LocalDate, Double)]()
      var curDate = start
      while (curDate.isBefore(end)) {
        if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
          cur = cur.tail
        }

        filled += ((curDate, cur.head._2))
        curDate = curDate.plusDays(1)

        // skip weekends!
        if (curDate.getDayOfWeek.getValue > 5) {
          curDate = curDate.plusDays(2)
        }
      }
      filled.toArray
    }

    def calculateReturnList(stocks: Array[(java.time.LocalDate, Double)] ): Array[ Double] = {
      val returns = new ArrayBuffer[Double]()
      for( a <- 0 to stocks.length-2){
         returns+= (stocks.apply(a+1)._2 - stocks.apply(a)._2)/stocks.apply(a)._2;  
      }
      return returns.toArray;
    }

    // filter out time series of less than 4 years
    val start = LocalDate.of(2009, 10, 23)
    val end = LocalDate.of(2014, 10, 23)

    val stocksDir = new File(dataDirectory + "/stocks/")
    val files = stocksDir.listFiles()


    //-------------- HISTORICAL METHOD -------------------


	val stockList = Seq("AAPL","AMZN","GOOGL","MSFT")
	println("\n\nCOMPUTING HISTORICAL 5% Var / CVar using sliding for 10 DAY (2 week) time horizon ")

	for(stockName <- stockList){
		
    val allStocks = files.filter{x => x.getName.contains(stockName)}.flatMap { file =>
      try {
        Some(readStockHistory(file))
      } catch {
        case e: Exception => None
      }
    }

    val rawStocks = allStocks.filter(_.size >= 260 * 5 + 10) // keep only stocks with more than 5 years of trading

    // trim and fill-in the stocks' and factors' time-series data
    val stocks = rawStocks.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))
  	
  	//Sort by Date
  	val stockDateSorted = stocks.apply(0).sortBy(_._1.toEpochDay)

    //Calculate Returns 
    var i = 0
    val returnsArr = stockDateSorted.sliding(10).map { window =>
        val next = window.last._2
        val prev = window.head._2
        i += 1
        (next - prev) / prev
      }.toArray

    println("\n\n5%-QUANTILE (VaR) for "+stockName+" :     " + "\t" + returnsArr.sorted.apply((returnsArr.size * 0.05).toInt))
    println("AVG OVER 5%-QUANTILE (CVar) for "+stockName+" : " + (returnsArr.sorted.toList.take((returnsArr.size * 0.05).toInt).sum / (returnsArr.size * 0.05).toInt))

	}






    

	//------------FOR MONTECARLO SIMULATION ----------------


	// read the 3 factor files
    val factorsPrefix = dataDirectory + "/factors/"
    val rawFactors = Array("NASDAQ-TLT.csv", "NYSEARCA-CRED.csv", "NYSEARCA-GLD.csv").
      map(x => new File(factorsPrefix + x)).
      map(readStockHistory)

    val factors = rawFactors.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))

     // calculate returns for sliding windows over 2 weeks of consecutive trading days
    def twoWeekReturns(history: Array[(LocalDate, Double)]): Array[Double] = {
      var i = 0
      // println("CALCULATING RETURNS OVER SLIDING WINDOW OF SIZE " + history.size)
      history.sliding(10).map { window =>
        val next = window.last._2
        val prev = window.head._2
        //println("\t" + i + "\t" + next + " - " + prev + " = " + (next - prev) / prev + "\t" + window.last._1 + " - " + window.head._1)
        i += 1
        (next - prev) / prev
      }.toArray
    }

    val factorReturns = factors.map(twoWeekReturns).toArray.toSeq

     def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
      val mat = new Array[Array[Double]](histories.head.length)
      for (i <- histories.head.indices) {
        mat(i) = histories.map(_(i)).toArray
      }
      mat
    }

    def featurize(factorReturns: Array[Double]): Array[Double] = {
      val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
      val squareRootedReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
      squaredReturns ++ squareRootedReturns ++ factorReturns
    }

    import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

    def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]]): OLSMultipleLinearRegression = {
      val regression = new OLSMultipleLinearRegression()
      regression.newSampleData(instrument, factorMatrix)
      regression
    }

    def computeFactorWeights(
      stocksReturns:  Seq[Array[Double]],
      factorFeatures: Array[Array[Double]]): Array[Array[Double]] = {
      stocksReturns.map(linearModel(_, factorFeatures)).map(_.estimateRegressionParameters()).toArray
    }

    import org.apache.commons.math3.stat.correlation.Covariance


    // put the factors returns into matrix form
    val factorMat = factorMatrix(factorReturns)

    // parameters of the multivariate normal distribution
    val factorMeans = factorReturns.map(factor => factor.sum / factor.size).toArray
    val factorCov = new Covariance(factorMat).getCovarianceMatrix().getData()

    // parameters of the linear-regression model
    val factorFeatures = factorMat.map(featurize)

    def instrumentTrialReturn(instrument: Array[Double], trial: Array[Double]): Double = {
      var instrumentTrialReturn = instrument(0)
      var i = 0
      while (i < trial.length) {
        instrumentTrialReturn += trial(i) * instrument(i + 1)
        i += 1
      }
      instrumentTrialReturn
    }

    // calculate the avg return of the portfolio under particular trial conditions
    def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]): Double = {
      var totalReturn = 0.0
      for (instrument <- instruments) {
        totalReturn += instrumentTrialReturn(instrument, trial)
      }
      totalReturn / instruments.size
    }

    import org.apache.commons.math3.random.MersenneTwister
    import org.apache.commons.math3.distribution.MultivariateNormalDistribution

    // calculate the returns for a repeated set of trials based on a given seed
    def trialReturns(

      seed:              Long,
      numTrials:         Int,
      instruments:       Seq[Array[Double]],
      factorMeans:       Array[Double],
      factorCovariances: Array[Array[Double]]): Seq[Double] = {

      // println("GENERATING " + numTrials + " TRIALS FOR SEED " + seed)
      val rand = new MersenneTwister(seed) // slightly more sophisticated random-number generator
      val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans,
        factorCovariances)

      val trialReturns = new Array[Double](numTrials)
      for (i <- 0 until numTrials) {
        val trialFactorReturns = multivariateNormal.sample()
        val trialFeatures = featurize(trialFactorReturns)
        trialReturns(i) = trialReturn(trialFeatures, instruments)
      }

      trialReturns
    }

    // set the parameters for parallel sampling
    val numTrials = 1000000
    val parallelism = 100
    val baseSeed = 1001L

    // generate different seeds so that our simulations don't all end up with the same results
    val seeds = (baseSeed until baseSeed + parallelism)

	println("\n\nCOMPUTING MONTECARLO 5% Var / CVar 10 DAY (2 week) time horizon :")

    for(stockName <- stockList){
		
    val allStocks = files.filter{x => x.getName.contains(stockName)}.flatMap { file =>
      try {
        Some(readStockHistory(file))
      } catch {
        case e: Exception => None
      }
    }

    val rawStocks = allStocks.filter(_.size >= 260 * 5 + 10) // keep only stocks with more than 5 years of trading

    // trim and fill-in the stocks' and factors' time-series data
    val stocks = rawStocks.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))

    val stockReturns = stocks.map(twoWeekReturns).toArray.toSeq
    val factorWeights = computeFactorWeights(stockReturns, factorFeatures)

    var trials = new ArrayBuffer[Double]()
    for (i <- 0 until seeds.size) {
      trials = trials ++ trialReturns(seeds(i), numTrials / parallelism, factorWeights, factorMeans, factorCov)
    }

    println("\n\n5%-QUANTILE (VaR) for "+stockName+" : " + trials.sorted.apply((trials.size * 0.05).toInt))
    println("AVG OVER 5%-QUANTILE (CVar) for "+stockName+" : " + (trials.sorted.toList.take((trials.size * 0.05).toInt).sum / (trials.size * 0.05).toInt))

  	
  	}
    

