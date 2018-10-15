package ikoda.ml

import grizzled.slf4j.Logging
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.{DataFrameUtils,  SparkConfProviderWithStreaming}
import ikoda.nlp.analysis.CollegeURLAnalyzerThread
import ikoda.utils.LibSvmProcessor
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
//import ikoda.nlp.analysis.CollegeURLAnalyzerThread
import ikoda.utils.Spreadsheet
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConverters._


@Test
class RandomForestTest extends Logging with SparkConfProviderWithStreaming
{
  
  /*****************************
  val sparseTrainingSet: RDDLabeledPoint = new RDDLabeledPoint
  
  
  @Test
  def testRF(): Unit =
  {
    try
    {
      logger.info("RF Test")

      
      //sparseTrainingSet.loadLibSvmPJ(spark, "./unitTestInput/urlrf/FINAL_C_trainingSet.libsvm")
      
      //runRF(spark)
      //lookAtModel(spark)
      
      //loadTestDataSet(spark)
      
      
      assertTrue(true)
    }
    catch
    {
      case ex: Exception =>
      {
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}")
      }
    }
  }
  
  @throws
  def loadTestDataSet(spark: SparkSession): Unit =
  {
    try
    {
      Spreadsheet.getInstance().initCsvSpreadsheet("testDataSet", "ikoda.ml", "./unitTestInput/urlrf")
      Spreadsheet.getInstance().getCsvSpreadSheet("testDataSet").loadCsv("CATEGORY_LOG.csv", "A_RowId")
      val testData = Spreadsheet.getInstance().getCsvSpreadSheet("testDataSet").getData.asScala
      val analyzer: CollegeURLAnalyzerThread = new CollegeURLAnalyzerThread()
      testData.foreach
      {
        entry =>
          
          val urlo: Option[String] = entry._2.asScala.get("FULL_URL")
          val categoryo: Option[String] = entry._2.asScala.get("CATEGORY")
          val uido: Option[String] = entry._2.asScala.get("A_RowId")
          
          if (categoryo.isDefined && urlo.isDefined && uido.isDefined)
          {
            val toAnalyze: String = s"${urlo.get} ${categoryo.get}"
            logger.debug(s"Analyzing $toAnalyze")
            val sparseResult: LibSvmProcessor = analyzer.getTokensFromUrlString(toAnalyze, "en")
            
            logger.debug(s"rows in result=${sparseResult.getData.size()}")
            logger.debug(s"cols in result=${sparseResult.getColumnHeadings.size()}")
            
            val sparse0: RDDLabeledPoint = sparseTrainingSet.transformToRDDLabeledPointWithMatchingSchema(spark, sparseResult)
            
            
            
            //val sparse0:RDDLabeledPoint=new RDDLabeledPoint
            //sparse0.loadLibSvmPJ(spark,"./unitTestInput/urlrf/FINAL_C_trainingSet.libsvm")
            
            
            logger.debug(s"asSparse rows: ${sparse0.getRowCountCollected}")
            //logger.debug(s"asSparse columns: ${sparse0.columnHeadsStringList()}")
            logger.debug(s"asSparse targets: ${sparse0.getTargets().mkString(", ")}")
            
            val prediction: String = predict(spark, sparse0)
            logger.info("PREDICTED "+prediction)
            Spreadsheet.getInstance().getCsvSpreadSheet("testDataSet").addCell(uido.get,"PREDICTION",prediction)
            

            
            
            
          }
          
        
        
      }
      Spreadsheet.getInstance().getCsvSpreadSheet("testDataSet").printCsvFinal("CATEGORY_LOG_PREDICTED")
      
      
    }
    catch
    {
      case ex: Exception =>
      {
        
        logger.error(ex.getMessage, ex)
        fail()
        
        throw new Exception(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}", ex)
      }
    }
  }
  
  @throws
  def predict(spark: SparkSession, sparse0: RDDLabeledPoint): String =
  {
    try
    {
     
      sparse0.getRowCountCollected match
      {
        case x if x > 1  =>
        {
          logger.warn(s"\nExpecting 1 row, but found ${sparse0.getRowCountCollected} rows")
          "NA"
        }
        case 0 =>
          {
        logger.warn(s"\nExpecting 1 row, but found 0 rows")
            "NA"
      }
        case _ =>
        {
          logger.info("\n\n--------\n\n")
          val data = sparse0.lpData()
    

    
    
          logger.info(s"Loading Model")
          val model = RandomForestModel.load(spark.sparkContext, "./unitTestOutput/urlrf/myRandomForestClassificationModel")
          logger.info(s"Predicting")

    
          val target: ArrayBuffer[Double] = new ArrayBuffer[Double]()
          data.collect().foreach
          {
      
            point =>
        
              System.out.println(point.features)
              System.out.println(point.features.toSparse.indices.mkString(","))
              System.out.println(point.features.toSparse.values.mkString(","))
        
              val d = model.predict(point.features)
              System.out.println(d)
        
              target += d
      
      
          }
          logger.info(s"---Predicting result count: ${target.size}")
    
          logger.info(target.mkString("--\n"))
    
          sparseTrainingSet.getTargetName(target(0))
        }
      }
    }
    catch
    {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        throw new Exception(ex.getMessage, ex)
    }
  }
  
  
  def lookAtModel(spark: SparkSession): Unit =
  {
    try
    {
      // Load and parse the data file, converting it to a DataFrame.
      val sparse0: RDDLabeledPoint = new RDDLabeledPoint
      sparse0.loadLibSvmPJ(spark, "./unitTestInput/urlrf/FINAL_C_trainingSet.libsvm")
      logger.info(s"Rows: ${sparse0.getRowCountCollected}")
      logger.info(s"Columns: ${sparse0.getColumnCount}")
      logger.info(s"Targets: ${sparse0.getTargets().size}")
      val data:RDD[LabeledPoint] = sparse0.lpData()
      //val data = MLUtils.loadLibSVMFile(spark.sparkContext, "C:/Users/jake/__workspace/scalaProjects/scalaML/unitTestInput/scalaLibsvmSample.libsvm")
      
      
      // Load and parse the data file.
      //val data = MLUtils.loadLibSVMFile(spark.sparkContext, "data/mllib/sample_libsvm_data.txt")
      // Split the data into training and test sets (30% held out for testing)
      val splits = data.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))
      
      
      logger.info(s"Loading Model")
      val model = RandomForestModel.load(spark.sparkContext, "./unitTestOutput/urlrf/myRandomForestClassificationModel")
      logger.info(s"Predicting")
      // Evaluate model on test instances and compute test error
      val labelAndPreds = testData.map
      { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      logger.info(s"labelAndPreds: ${labelAndPreds}")
      
      val output: Array[String] = labelAndPreds.map
      {
        r => String.valueOf(s"${r._1} ${sparse0.getTargetName(r._1.toInt)}  :  ${r._2} ${sparse0.getTargetName(r._2.toInt)}\n)")
      }.collect()
      
      
      logger.info(s"Output: ${output.mkString("\n")}")
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
      logger.info("\n\n\nTest Error = " + testErr+"\n\n\n")
      logger.info("Learned classification forest model:\n" + model.toDebugString)
      
      
      assertTrue(true)
    }
    catch
    {
      case ex: Exception =>
      {
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}", ex)
        fail()
      }
    }
  }
  
  
  def runRF(spark: SparkSession): Unit =
  {
    try
    {
      // Load and parse the data file, converting it to a DataFrame.
      
      logger.info(s"Rows: ${sparseTrainingSet.getRowCountCollected}")
      logger.info(s"Columns: ${sparseTrainingSet.getColumnCount}")
      logger.info(s"Targets: ${sparseTrainingSet.getTargets().size}")
      val data = sparseTrainingSet.lpData()
      //val data = MLUtils.loadLibSVMFile(spark.sparkContext, "C:/Users/jake/__workspace/scalaProjects/scalaML/unitTestInput/scalaLibsvmSample.libsvm")
      
      
      // Load and parse the data file.
      //val data = MLUtils.loadLibSVMFile(spark.sparkContext, "data/mllib/sample_libsvm_data.txt")
      // Split the data into training and test sets (30% held out for testing)
      val splits = data.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))
      
      // Train a RandomForest model.
      // Empty categoricalFeaturesInfo indicates all features are continuous.
      val numClasses = 58
      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 8 // Use more in practice.
      val featureSubsetStrategy = "auto" // Let the algorithm choose.
      val impurity = "gini"
      val maxDepth = 30
      val maxBins = 32
      
      val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
      
      // Evaluate model on test instances and compute test error
      
      
      // Save and load model
      DataFrameUtils.deletePartition("./unitTestOutput/urlrf/myRandomForestClassificationModel")
      model.save(spark.sparkContext, "./unitTestOutput/urlrf/myRandomForestClassificationModel")
      
      
      assertTrue(true)
    }
    catch
    {
      case ex: Exception =>
      {
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}", ex)
        assertTrue(false)
      }
    }
  }
  
  *********************/
}


