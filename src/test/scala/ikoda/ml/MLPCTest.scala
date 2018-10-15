package ikoda.ml

import java.util

import grizzled.slf4j.Logging
import ikoda.ml.predictions.MLPCPredictions
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.{DataFrameUtils, SparkConfProvider, SparkConfProviderWithStreaming}
import ikoda.nlp.analysis.CollegeURLAnalyzerThread
import ikoda.utils.LibSvmProcessor
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.sql.DataFrame
//import ikoda.nlp.analysis.CollegeURLAnalyzerThread
import ikoda.utils.Spreadsheet
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConverters._


@Test
class MLPCTest extends Logging with SparkConfProviderWithStreaming
{
  /******
  
  val sparseTrainingSet: RDDLabeledPoint = new RDDLabeledPoint
  
  
  @Test
  def testMLPC(): Unit =
  {
    try
    {


     //predictOne
      logger.info("RF Test")

      
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
  def predictOne(): Unit =
  {
    try
    {
      val p: MLPCPredictions = new MLPCPredictions
      var hm: java.util.HashMap[String, String] = new util.HashMap[String, String]()
      import ikoda.ml.predictions.MLPCPredictions
      val title = "Academic Programs Table - Moraine Valley Community College"
      val rowId = "1619"
      val url = "http:||morainevalley.smartcatalogiq.com|en|2017-2018|Catalog|Career-Programs|Business-Administration-Associate|Accounting-Assistant-Clerk-Certificate\\n"
      hm.put(MLPCPredictions.title, title)
      hm.put(MLPCPredictions.url, url)
      hm.put(MLPCPredictions.rowId, rowId)
      logger.info("predictOne" + p.predictMajorbyUrlData(hm))

      val title1 = "Academic Programs Table - Moraine Valley Community College"
      val rowId1 = "1619"
      val url1 = "http:||morainevalley.smartcatalogiq.com|en|2017-2018|Catalog|Career-Programs|Fitness-Associate|Coaching Training Exercise\\n"
      hm.put(MLPCPredictions.title, title1)
      hm.put(MLPCPredictions.url, url1)
      hm.put(MLPCPredictions.rowId, rowId1)
      logger.info("predictOne" + p.predictMajorbyUrlData(hm))
    }
    catch
    {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
        fail()
  
        throw new Exception(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}", ex)
    }
  }
  
  @throws
  def loadTestDataSet(spark: SparkSession): Unit =
  {
    try
    {
      Spreadsheet.getInstance().initCsvSpreadsheet("testDataSet", "ikoda.ml", "./unitTestInput/urlmlpc")
      Spreadsheet.getInstance().getCsvSpreadSheet("testDataSet").loadCsv("CATEGORY_LOG.csv", "A_RowId")
      val testData = Spreadsheet.getInstance().getCsvSpreadSheet("testDataSet").getData.asScala
      val analyzer: CollegeURLAnalyzerThread = new CollegeURLAnalyzerThread()
      logger.info(s"Loading Model")
      val model = MultilayerPerceptronClassificationModel.load( "./unitTestOutput/urlmlpc/MLPCModel")
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

            val sparse0: RDDLabeledPoint = sparseTrainingSet.transformToRDDLabeledPointWithMatchingSchema(spark, sparseResult)

            val predictiono: Option[Double] = predict(spark, sparse0.getMLPackageLabeledPointsAsDataframe(spark),model)
            if(predictiono.isDefined)
            {
              val prediction:String=sparse0.getTargetName(predictiono.get)
              logger.info("PREDICTED " + prediction)
              Spreadsheet.getInstance().getCsvSpreadSheet("testDataSet").addCell(uido.get,"PREDICTION",prediction)
            }
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
  def predict(spark: SparkSession, df0: DataFrame,model:MultilayerPerceptronClassificationModel): Option[Double] =
  {
    try
    {
  
      df0.count() match
      {
        case x if x > 1 =>
        {
          logger.warn(s"\nExpecting 1 row, but found ${df0.count()} rows")
          None
        }
        case 0 =>
        {
          logger.warn(s"\nExpecting 1 row, but found 0 rows")
          None
        }
        case _ =>
        {
          //logger.info("\n\n--------\n\n")
  
  

          val result = model.transform(df0)
          result.show()
          val predictionAndLabels = result.select("prediction", "label")
          predictionAndLabels.show()
          Option(predictionAndLabels.head.getDouble(0))
         
          
          
  
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
      sparse0.loadLibSvmPJ(spark, "./unitTestInput/urlmlpc/FINAL_C_trainingSet.libsvm")
      logger.info(s"Rows: ${sparse0.getRowCountEstimate}")
      logger.info(s"Columns: ${sparse0.getColumnCount}")
      logger.info(s"Targets: ${sparse0.getTargets().size}")
      val data = sparse0.getMLPackageLabeledPointsAsDataframe(spark)
      //val data = MLUtils.loadLibSVMFile(spark.sparkContext, "C:/Users/jake/__workspace/scalaProjects/scalaML/unitTestInput/scalaLibsvmSample.libsvm")
      
      
      // Load and parse the data file.
      //val data = MLUtils.loadLibSVMFile(spark.sparkContext, "data/mllib/sample_libsvm_data.txt")
      // Split the data into training and test sets (30% held out for testing)
      val splits = data.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))
      
      
      logger.info(s"Loading Model")
      val model = MultilayerPerceptronClassificationModel.load( "./unitTestOutput/urlmlpc/MLPCModel")
      logger.info(s"Predicting")
      // Evaluate model on test instances and compute test error
      val result = model.transform(testData)
      val predictionAndLabels = result.select("prediction", "label")

      logger.info("Results:")

      import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
      val evaluator1 = new MulticlassClassificationEvaluator().setMetricName("accuracy")
      val evaluator2 = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision")
      val evaluator3 = new MulticlassClassificationEvaluator().setMetricName("weightedRecall")
      val evaluator4 = new MulticlassClassificationEvaluator().setMetricName("f1")
      logger.info("Accuracy = " + evaluator1.evaluate(predictionAndLabels))
      logger.info("Precision = " + evaluator2.evaluate(predictionAndLabels))
      logger.info("Recall = " + evaluator3.evaluate(predictionAndLabels))
      logger.info("F1 = " + evaluator4.evaluate(predictionAndLabels))
      logger.info("Saving Results")
      DataFrameUtils.saveToPjCsv(predictionAndLabels,"MLPCPredictions","./unitTestOutput/urlmlpc")
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
  
  
  def  runMLPC(spark: SparkSession): Unit =
  {
    try
    {
      // Load and parse the data file, converting it to a DataFrame.
      
      logger.info(s"Rows: ${sparseTrainingSet.getRowCountEstimate}")
      logger.info(s"Columns: ${sparseTrainingSet.getColumnCount}")
      logger.info(s"Targets: ${sparseTrainingSet.getTargets().size}")
      //val data = sparseTrainingSet.getLibsvmDataAsDataframePJ(spark)
      //val data = MLUtils.loadLibSVMFile(spark.sparkContext, "C:/Users/jake/__workspace/scalaProjects/scalaML/unitTestInput/scalaLibsvmSample.libsvm")

      val data=sparseTrainingSet.convertToMLPackageRDD(spark)
      // Load and parse the data file.
      //val data = MLUtils.loadLibSVMFile(spark.sparkContext, "data/mllib/sample_libsvm_data.txt")
      // Split the data into training and test sets (30% held out for testing)
      val splits = data.randomSplit(Array(0.9, 0.1))
      val (trainingData, testData) = (splits(0), splits(1))


      val layers = Array[Int](sparseTrainingSet.getColumnCount, 24, 16, sparseTrainingSet.getTargets().size)
      // Train a RandomForest model.
      // Empty categoricalFeaturesInfo indicates all features are continuous.
      logger.info("Training set up")
      val trainer:MultilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
        .setLayers(layers) // Sets the value of param [[layers]].                .
        .setTol(1E-4) //Set the convergence tolerance of iterations. Smaller value will lead to higher accuracy with the cost of more iterations. Default is 1E-4.
        .setBlockSize(128) // Sets the value of param [[blockSize]], where, the default is 128.
        .setSeed(12345L) // Set the seed for weights initialization if weights are not set
        .setMaxIter(100); // Set the maximum number of iterations. Default is 100.

      import spark.implicits._
      val dfTrain=trainingData.toDF()
      val dfTest = testData.toDF()
      logger.info("Training .......")
      val model = trainer.fit(dfTrain)
      // Evaluate model on test instances and compute test error
      
      
      // Save and load model
      logger.info("Saving Model")
      DataFrameUtils.deletePartition("./unitTestOutput/urlmlpc/MLPCModel")
      model.save( "./unitTestOutput/urlmlpc/MLPCModel")
      val result = model.transform(dfTest)
      val predictionAndLabels = result.select("prediction", "label")

      logger.info("Results:")

      import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
      val evaluator1 = new MulticlassClassificationEvaluator().setMetricName("accuracy")
      val evaluator2 = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision")
      val evaluator3 = new MulticlassClassificationEvaluator().setMetricName("weightedRecall")
      val evaluator4 = new MulticlassClassificationEvaluator().setMetricName("f1")
      logger.info("Accuracy = " + evaluator1.evaluate(predictionAndLabels))
      logger.info("Precision = " + evaluator2.evaluate(predictionAndLabels))
      logger.info("Recall = " + evaluator3.evaluate(predictionAndLabels))
      logger.info("F1 = " + evaluator4.evaluate(predictionAndLabels))
      logger.info("Saving Results")
      DataFrameUtils.saveToPjCsv(predictionAndLabels,"MLPCPredictions","./unitTestOutput/urlmlpc")
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
  ********/
  
}


