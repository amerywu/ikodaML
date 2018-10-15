package ikoda.ml.predictions

import java.io.File

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.SparkDirectoryFinder
import ikoda.ml.cassandra.SparseDataToRDDLabeledPoint
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.nlp.analysis.CollegeURLAnalyzerThread
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.SparkConfProviderWithStreaming
import ikoda.utils.LibSvmProcessor
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

object MLPCPredictions
{
  val title:String="Title"
  val url:String="Url"
  val rowId:String="RowId"
  val mlpcModel="mlpcModel"
  val mlpcDataSchema="mlpcDataSchema"
  val analyzerClass="analyzer"
}

/**
  * Predicts the category of a web page based on its URL and title
  */
class MLPCPredictions extends Logging with SparkConfProviderWithStreaming
{



  private val modelRootDiro:Option[String] = Some(ConfigFactory.load("scalaML").getString("scalaML.modelsRoot.value"))
  private val mlpcPredictByUrlModelNameo:Option[String] = Some(ConfigFactory.load("scalaML").getString("scalaML.mlpcPredictByUrlModelName.value"))
  private val fieldVariables:mutable.HashMap[String,Any]=new mutable.HashMap
  val sparkSession=getSparkStreamingContext()


  def initialize()
  {
    getMlpcPredictByUrlModel
    getMlpcDataSchema()
    getAnalyzer()
  }
  
  
  @throws(classOf[IKodaMLException])
  private def getAnalyzer():CollegeURLAnalyzerThread=
  {
    try
    {
      if (fieldVariables.get(MLPCPredictions.analyzerClass).isDefined)
      {
        fieldVariables.get(MLPCPredictions.analyzerClass).get.asInstanceOf[CollegeURLAnalyzerThread]
      }
      else
           {
             val analyzer: CollegeURLAnalyzerThread = new CollegeURLAnalyzerThread()
             fieldVariables.put(MLPCPredictions.analyzerClass, analyzer)
  
             getAnalyzer
           }
    }
    catch
    {
      case ex:Exception => throw new IKodaMLException(ex.getMessage,ex)
    }
  }
  

  
  @throws(classOf[IKodaMLException])
  private def getMlpcPredictByUrlModel():MultilayerPerceptronClassificationModel=
  {
    try
    {

      if (fieldVariables.get(MLPCPredictions.mlpcModel).isDefined)
      {
        fieldVariables.get(MLPCPredictions.mlpcModel).get.asInstanceOf[MultilayerPerceptronClassificationModel]
      }
      else if (modelRootDiro.isDefined && mlpcPredictByUrlModelNameo.isDefined)
      {

        val mlpcPredictByUrlModel = MultilayerPerceptronClassificationModel.load(s"${modelRootDiro.get}${File.separator}${mlpcPredictByUrlModelNameo.get}")
        fieldVariables.put(MLPCPredictions.mlpcModel,mlpcPredictByUrlModel)
        getMlpcPredictByUrlModel
      }
      else
      {
        logger.warn("Could not load model. Configuration modelsRoot  and/or mlpcPredictByUrlModelName may be undefined or incorreclty defined in scalaML.conf")
        throw new IKodaMLException("Could not load model. Configuration modelsRoot  and/or mlpcPredictByUrlModelName may be undefined or incorreclty in scalaML.conf")
      }
    }
    catch
      {
        case ex:Exception => throw new IKodaMLException(ex.getMessage,ex)
      }
  }


  @throws(classOf[IKodaMLException])
  private def getMlpcDataSchema():Option[RDDLabeledPoint]=
  {
    var ks:String=""
    try
    {
      if (fieldVariables.get(MLPCPredictions.mlpcDataSchema).isDefined)
      {
        fieldVariables.get(MLPCPredictions.mlpcDataSchema).asInstanceOf[Option[RDDLabeledPoint]]
      }
      else
        {
           ks=ConfigFactory.load("scalaML").getString("scalaML.keyspace.collegeurlmodel")
          val pconfig:PipelineConfiguration=new PipelineConfiguration
          pconfig.config(PipelineConfiguration.keyspaceName,ks)
          val loader:SparseDataToRDDLabeledPoint=new SparseDataToRDDLabeledPoint(pconfig)
          val sparseTrainingSet=loader.loadFromCassandra
          logger.info(sparseTrainingSet.info)
          fieldVariables.put(MLPCPredictions.mlpcDataSchema,sparseTrainingSet)
          Some(sparseTrainingSet)
        }
    }
    catch
    {
      case ex:Exception =>
        logger.warn(s"There is no data schema available at ${ks}   ${ex.getMessage}" )
        None
    }
  }


  private def runPrediction(urlo:Option[String],categoryo:Option[String], uido:Option[String]): String =
  {
    val toAnalyze: String = s"${urlo.get} ${categoryo.get}"
    val analyzer: CollegeURLAnalyzerThread = getAnalyzer()
    logger.info(s"\n---------------------------\n\tAnalyzing $toAnalyze\n---------------------------\n")
    val sparseResult: LibSvmProcessor = analyzer.getTokensFromUrlString(toAnalyze, "en")
    sparseResult.columnsToLowerCase()
    logger.info("Tokenized "+sparseResult.getColumnHeadings)
    val sparseConverter: RDDLabeledPoint = getMlpcDataSchema.get
    val sparse0: RDDLabeledPoint = sparseConverter.transformToRDDLabeledPointWithSchemaMatchingThis( sparseResult)
    //logger.debug(sparse0.getColumnIndexNameMap().mkString("\n"))
    //logger.debug("Should be one row:"+sparse0.info)
    sparse0.lpData().collect().foreach(lp=>logger.debug("["+lp.features.toSparse.indices.mkString(",")+"]"))

    val predictiono:Option[Double] = predict(spark, sparse0.transformRDDToDataFrame(),getMlpcPredictByUrlModel)

    if(predictiono.isDefined)
    {
      val prediction:String=sparse0.getTargetName(predictiono.get)
      logger.info("PREDICTED " + prediction)
      prediction
    }
    else
    {
      s"FAILED: No prediction received"
    }
  }
  
  def predictMajorbyUrlData(inputDataj:java.util.HashMap[String,String]): String =
  {
       initialize()
      val inputData=inputDataj.asScala
      val urlo: Option[String] = inputData.get(MLPCPredictions.url)
      val categoryo: Option[String] = inputData.get(MLPCPredictions.title)
      val uido: Option[String] = inputData.get(MLPCPredictions.rowId)

      if(!getMlpcDataSchema.isDefined)
      {
        "FAILED No schema available on Spark"
      }
      else if (categoryo.isDefined && urlo.isDefined && uido.isDefined)
      {
        runPrediction(urlo,categoryo, uido)
      }
      else
      {
        s"FAILED: Input Data Not Defined"
      }
  }
  
  @throws
  private def predict(spark: SparkSession, df0: DataFrame,model:MultilayerPerceptronClassificationModel): Option[Double] =
  {
    try
    {
      df0.count() match
      {
        case x if x > 1 =>
        {
          logger.warn (s"\nExpecting 1 row, but found ${df0.count()} rows")
          None
        }
        case 0 =>
        {
          logger.warn(s"\nExpecting 1 row, but found 0 rows")
          None
        }
        case _ =>
        {
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
}
