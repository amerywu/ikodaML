package ikoda.ml.pipeline

import ikoda.IKodaMLException
import ikoda.ml.pipeline.PipelineConfiguration.{km_termRepeatAcrossClusters, phraseAnalysisOverwriteClusterCsv}
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.SparkConfProviderWithStreaming
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, types}

import scala.collection.mutable

/**
  * Specifies legal configuration parameters for a pipeline.
  *
  */
object PipelineConfiguration
{
  val rawDataPath = "rawDataPath"
  val _mainMethodMap ="_mainMethodMap"
  val _subsetMethodMap ="_subsetMethodMap"
  val pipelineOutputRoot = "pipelineOutputRoot"
  val mergeMapPropertiesFile="mergeMapPropertiesFile"
  val keyspaceName="keyspaceName"
  val keyspaceUUID="keyspaceUUID"
  val analysisType="analysisType"
  val printTruncateValues="printTruncateValues"
  val printStageName="printStageName"
  val prefixForMerging="prefixForMerging"
  val prefixForPrinting="prefixForPrinting"
  val lda_topicCount = "lda_topicCount"
  val lda_topicCountByTarget = "lda_topicCountByTarget"
  val km_clusterCount = "km_clusterCount"
  val km_clusterCountByTarget = "km_clusterCountByTarget"
  val km_countTopClusterValues = "km_countTopClusterValues"
  val km_minTopicWeight = "km_minTopicWeight"
  val km_minTopicWeightByTarget = "km_minTopicWeightByTarget"
  val km_termRepeatAcrossClusters:String="km_termRepeatAcrossClusters"
  val km_iterations:String="km_iterations"
  val km_iterationsByTarget:String="km_iterationsByTarget"
  val km_initSteps:String="km_initSteps"
  val km_initStepsByTarget:String="km_initStepsByTarget"
  val lda_minTopicWeight = "lda_minTopicWeight"
  val lda_minTopicWeightByTarget = "lda_minTopicWeightByTarget"
  val rr_minEntryCountPerTarget = "rr_minEntryCountPerTarget"
  val rr_maxDuplicateProportionPerTarget = "rr_maxDuplicateProportionPerTarget"
  val cr_stopwords="cr_stopwords"
  val cr_lowFrequencyPercentilToRemove = "cr_lowFrequencyPercentilToRemove"
  val cr_medianOffsetForLowFrequency = "cr_medianOffsetForLowFrequency"
  val simpleLogName = "simpleLogName"
  val lda_termRepeatAcrossTopics:String="lda_termRepeatAcrossTopics"
  val cr_highFrequencyPercentileToRemove:String="cr_highFrequencyPercentileToRemove"
  val phraseAnalysisReportPath:String="phraseAnalysisReportPath"
  val phraseAnalysisDataSourceRootPath:String="phraseAnalysisDataSourceRootPath"
  val phraseAnalysisFileNamePrefix:String="phraseAnalysisFileNamePrefix"
  val phraseAnalysisInputCsvUidColumnName:String="phraseAnalysisInputCsvUidColumnName"
  val phraseAnalysisSecondTierFileName:String="phraseAnalysisSecondTierFileName"
  val phraseAnalysisDataSourceFileName:String="phraseAnalysisDataSourceFileName"
  val phraseAnalysisTopTermCountPerClusterForAnalysis:String="phraseAnalysisTopTermCountPerClusterForAnalysis"
  val phraseAnalysisAllTargetsColValue:String="phraseAnalysisAllTargetsColValue"
  //val phraseAnalysisWhichPhraseAnalysisDataSets:String="optionPhraseAnalysisWhichGroupingData"
  val phraseCodingCleanOutputFile:String="phraseCodingCleanOutputFile"
  val phraseCodingTrainingDataFile:String="phraseCodingTrainingDataFile"
  val phraseCodingModelRootDir:String="phraseCodingModelRootDir"
  val randomSubsetProportion:String="randomSubsetProportion"
  val phraseAnalysisOverwriteClusterCsv:String="phraseAnalysisOverwriteClusterCsv"
  val lda_countTopClusterValues:String="lda_countTopClusterValues"
  val pathToSpark:String="pathToSpark"




  val names:Map[String,String] = Map(
    _mainMethodMap -> "The list of functions applied to the entire data set",
    _subsetMethodMap -> "The list of functions applied to each subset/target",
    km_termRepeatAcrossClusters -> "When reducing by kmeans, the number of clusters a term can appear in the top x terms, before being removed from the dataset for subsequent iterations",
    lda_termRepeatAcrossTopics -> "When reducing by LDA, the number of topics a term can appear in the top x terms, before being removed from the dataset for subsequent iterations",
    rawDataPath -> "Path to the raw data file, used if loading from a file rather than Cassandra",
    pipelineOutputRoot -> "The folder where output files are saved",
    keyspaceName -> "Name of Cassandra keyspace",
    keyspaceUUID -> "Cassandra keyspace UUID",
    prefixForMerging -> "deprecated",
    prefixForPrinting -> "Part of the filename when saving libsvm data",
    mergeMapPropertiesFile -> "Name of file that maps how similar targets are merged",
    lda_topicCount -> "Number of LDA topics for entire dataset",
    lda_topicCountByTarget -> "Number of LDA topics per subset/target",
    km_clusterCount -> "Number of k means clusters for the entire dataset",
    km_clusterCountByTarget -> "Number of k means clusters per subset/target",
    km_iterations -> "Number of k means iterations",
    km_iterationsByTarget -> "Number of k means iterations per target",
    km_minTopicWeight -> "Minimum cluster weight for k means reporting",
    km_minTopicWeightByTarget -> "Minimum cluster weight for k means reporting by subset/target",
    km_initStepsByTarget -> "k means init steps on subset/target",
    km_initSteps -> "k means init steps",
    km_countTopClusterValues -> "How many top terms to report in k means",
    lda_minTopicWeight -> "Minimum topic weight for LDA reporting",
    lda_minTopicWeightByTarget -> "Minimum topic weight for LDA reporting by subset",
    lda_countTopClusterValues -> "How many top terms to report in LDA",
    rr_minEntryCountPerTarget -> "Remove all rows for targets where row count is less than x",
    rr_maxDuplicateProportionPerTarget -> "Proportion of permissible rows in a given target",
    cr_lowFrequencyPercentilToRemove -> "Low frequency percentile to remove",
    cr_highFrequencyPercentileToRemove -> "High frequency percentile to remove (e.g., 99)",
    simpleLogName -> "Name of the output log file",
    cr_medianOffsetForLowFrequency -> "If -1 then all columns with a total frequency < (median -1) will be rmoved ",
    cr_stopwords -> "List of high frequency words removed",
    phraseAnalysisReportPath -> "",
    phraseAnalysisDataSourceRootPath -> "",
    phraseAnalysisFileNamePrefix -> "",
    phraseAnalysisInputCsvUidColumnName -> "",
    phraseAnalysisDataSourceFileName -> "",
    phraseAnalysisTopTermCountPerClusterForAnalysis -> "",
    phraseAnalysisAllTargetsColValue -> "",
    //phraseAnalysisWhichPhraseAnalysisDataSets,
    phraseAnalysisSecondTierFileName -> "",
    phraseCodingCleanOutputFile -> "",
    phraseCodingTrainingDataFile -> "",
    phraseCodingModelRootDir -> "",
    phraseAnalysisOverwriteClusterCsv -> "",
    analysisType -> "The name of the current pipeline",
    randomSubsetProportion -> "",
    printTruncateValues -> "How many decimal points to keep when truncating for libsvm file",
    printStageName -> "Name of current stage. Used for fileNames",
    pathToSpark -> "Path from root to spark deployment directory"

  )
}
/***
  * Holds  configuration parameter values for a pipeline.
  *
  * Sets default values upon instantiation
  *
  * [[CustomPipelineConfiguration]] sets customized parameter values
  *
  * [[CustomPipelineConfiguration]] passes in method sequences, including for subsequences
  *
  * [[PipelineConfiguration]].runPipeline starts the pipeline. It calls each method in sequence, passing the returned dataset from each method as the input parameter to the next.
  */
class PipelineConfiguration extends PipelineMethods  with SparkConfProviderWithStreaming with Serializable
{
  private[this] lazy val options = defaultValues
  private val methodMaps:mutable.HashMap[String, scala.collection.immutable.SortedMap[Int, Option[ikoda.sparse.RDDLabeledPoint] => Option[ikoda.sparse.RDDLabeledPoint]]]=new mutable.HashMap[String, scala.collection.immutable.SortedMap[Int, Option[ikoda.sparse.RDDLabeledPoint] => Option[ikoda.sparse.RDDLabeledPoint]]]()
  private val methodMapDataFrame:mutable.HashMap[String, scala.collection.immutable.SortedMap[Int, Option[DataFrame] => Option[DataFrame]]]=new mutable.HashMap[String, scala.collection.immutable.SortedMap[Int, Option[DataFrame] => Option[DataFrame]]]()



  private[this] def defaultValues(): scala.collection.mutable.HashMap[String, String] =
  {
    val map:scala.collection.mutable.HashMap[String, String] = new scala.collection.mutable.HashMap[String, String]
    map.put(PipelineConfiguration.phraseAnalysisOverwriteClusterCsv,"true")
    map.put(PipelineConfiguration.simpleLogName,s"outputLog${System.currentTimeMillis()}")
    map.put(PipelineConfiguration.km_termRepeatAcrossClusters,"2")
    map.put(PipelineConfiguration.lda_termRepeatAcrossTopics,"2")
    map.put(PipelineConfiguration.mergeMapPropertiesFile,"/targetsMap.properties")
    map.put(PipelineConfiguration.lda_topicCount,"20")
    map.put(PipelineConfiguration.lda_topicCountByTarget,"12")
    map.put(PipelineConfiguration.km_iterations,"50")
    map.put(PipelineConfiguration.km_iterationsByTarget,"30")
    map.put(PipelineConfiguration.km_clusterCount,"20")
    map.put(PipelineConfiguration.km_clusterCountByTarget,"12")
    map.put(PipelineConfiguration.km_initSteps,"10")
    map.put(PipelineConfiguration.km_initStepsByTarget,"5")
    map.put(PipelineConfiguration.km_clusterCountByTarget,"12")
    map.put(PipelineConfiguration.km_countTopClusterValues,"15")
    map.put(PipelineConfiguration.lda_countTopClusterValues,"15")
    map.put(PipelineConfiguration.rr_minEntryCountPerTarget,"1000")
    map.put(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,"0")
    map.put(PipelineConfiguration.phraseAnalysisFileNamePrefix,"")
    map.put(PipelineConfiguration.phraseAnalysisInputCsvUidColumnName,"A_RowId")
    map.put(PipelineConfiguration.phraseAnalysisTopTermCountPerClusterForAnalysis,"5")
    map.put(PipelineConfiguration.phraseAnalysisSecondTierFileName,"secondTier.csv")
    map.put(PipelineConfiguration.randomSubsetProportion,"0.01")
    map.put(PipelineConfiguration.phraseAnalysisAllTargetsColValue,"ALL")
    map.put(PipelineConfiguration.cr_medianOffsetForLowFrequency,"0")
    map.put(PipelineConfiguration.cr_highFrequencyPercentileToRemove,"99.5")
    map.put(PipelineConfiguration.cr_lowFrequencyPercentilToRemove,"25")
    map.put(PipelineConfiguration.printTruncateValues,"6")
    map.put(PipelineConfiguration.printStageName,"UnspecifiedStage")
    map.put(PipelineConfiguration.prefixForPrinting,"")
    map.put(PipelineConfiguration.pathToSpark,"/home/jake/environment/spark-2.3.2-bin-hadoop2.7/")
    map
  }

  def passValue(pconfigin:PipelineConfiguration,key:String): Unit =
  {
    config(key,pconfigin.get(key))
  }

  def passValue(key:String,value:String): Unit =
  {
    config(key,value)
  }

  @throws(classOf[IKodaMLException])
  def validConfigOption(key: String): Unit =
  {
    if (!PipelineConfiguration.names.keySet.contains(key))
    {
      throw new IKodaMLException(s"$key is an invalid option. Valid options are ${PipelineConfiguration.names.mkString(",")}")
    }
  }
  
  def showOptions():String =
  {
    val sb:StringBuilder=new mutable.StringBuilder()
    options.toList.sorted.foreach
    {
      e =>
        sb.append(e._1)
        sb.append(":\t")
        sb.append(e._2)
        sb.append(":\n\t")
        sb.append(PipelineConfiguration.names.get(e._1).getOrElse(""))
        sb.append("\n\n")
    }
    sb.toString
  }

  def methodsForPipe(key:String):Option[scala.collection.immutable.SortedMap[Int, Option[ikoda.sparse.RDDLabeledPoint] => Option[ikoda.sparse.RDDLabeledPoint]]]=
  {
        methodMaps.get(key)
  }
  
  def methodOrder(key:String, inMap: scala.collection.immutable.SortedMap[Int, Option[ikoda.sparse.RDDLabeledPoint] => Option[ikoda.sparse.RDDLabeledPoint]]): Unit =
  {
    methodMaps.put(key,inMap)
  }

  def methodOrderDataFrame(key:String, inMap: scala.collection.immutable.SortedMap[Int, Option[DataFrame] => Option[DataFrame]]): Unit =
  {
    methodMapDataFrame.put(key,inMap)
  }
  
  @throws(classOf[IKodaMLException])
  def runPipeline(key:String): Unit =
  {
    try
    {
      val methodOrderMap=methodMaps.get(key)
      methodOrderMap.isDefined match
        {
        case true =>
          var oSparse: Option[RDDLabeledPoint] = Some(new RDDLabeledPoint)
          methodOrderMap.get.foreach
          {
            method =>
              oSparse = method._2(oSparse)
          }
        case false =>
          val methodOrderMapDataFrame=methodMapDataFrame.get(key)
          methodOrderMapDataFrame.isDefined match {
            case true =>
              val schema:StructType=StructType(Seq())

              var oDf: Option[DataFrame]= Some(spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema))
              methodOrderMapDataFrame.get.foreach
              {
                method =>
                  oDf = method._2(oDf)
              }
            case false =>
              logger.warn("\n\nNo methods specified for "+key+"\n")
          }




      }
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }
  
  
  @throws
  def config(key: String, value: String): PipelineConfiguration = synchronized
  {
    validConfigOption(key)
    options += key -> value
    this
  }
  
  @throws
  def config(key: String, value: Int): PipelineConfiguration = synchronized
  {
    validConfigOption(key)
    options += key -> value.toString
    this
  }
  
  @throws
  def config(key: String, value: Double): PipelineConfiguration = synchronized
  {
    validConfigOption(key)
    options += key -> value.toString
    this
  }
  
  @throws(classOf[IKodaMLException])
  def get(key: String): String =
  {
    val o: Option[String] = options.get(key)
    if (o.isDefined)
    {
      o.get
    }
    else
    {
      throw new IKodaMLException(s"$key is not defined")
    }
  }
  
  @throws(classOf[IKodaMLException])
  def getAsInt(key: String): Int =
  {
    try
    {
      val o: Option[String] = options.get(key)
      if (o.isDefined)
      {
        o.get.toInt
      }
      else
      {
        throw new IKodaMLException(s"$key is not defined")
      }
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  def getAsSeq(key: String): Seq[String] =
  {
    try
    {
      val o: Option[String] = options.get(key)
      if (o.isDefined)
      {
        o.get.split(",")
      }
      else
      {
        throw new IKodaMLException(s"$key is not defined")
      }
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }

  }
  
  def getAsBoolean(key: String): Boolean =
  {
    try
    {
      val o: Option[String] = options.get(key)
      if (o.isDefined)
      {
        if (o.get.equalsIgnoreCase("true") || o.get.equalsIgnoreCase("t"))
        {
          true
        }
        else
        {
          false
        }
      }
      else
      {
        throw new IKodaMLException(s"$key is not defined")
      }
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }
  
  
  @throws(classOf[IKodaMLException])
  def getAsDouble(key: String): Double =
  {
    try
    {
      val o: Option[String] = options.get(key)
      if (o.isDefined)
      {
        o.get.toDouble
      }
      else
      {
        throw new IKodaMLException(s"$key is not defined")
      }
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def getAsOption(key: String): Option[String] =
  {
    try
    {
         options.get(key)
    }
    catch
      {
        case e: Exception => throw new IKodaMLException(e.getMessage, e)
      }
  }
}
