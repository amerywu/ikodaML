package ikoda.ml.pipeline

import ikoda.IKodaMLException
import ikoda.ml.pipeline.PipelineConfiguration.{km_termRepeatAcrossClusters, phraseAnalysisOverwriteClusterCsv}
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.SparkConfProviderWithStreaming

import scala.collection.mutable

/***
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
  val reducedAndJoinedFileName = "reducedAndJoinedFileName"
  val prefixForMerging="prefixForMerging"
  val prefixForPrinting="prefixForPrinting"
  val targetColumn = "targetColumn"
  val uidCol = "uidCol"
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




  val names = Seq(
    _mainMethodMap,
    _subsetMethodMap,
    km_termRepeatAcrossClusters,
    lda_termRepeatAcrossTopics,
    rawDataPath,
    pipelineOutputRoot,
    keyspaceName,
    keyspaceUUID,
    prefixForMerging,
    prefixForPrinting,
    mergeMapPropertiesFile,
    targetColumn,
    uidCol,
    reducedAndJoinedFileName,
    lda_topicCount,
    lda_topicCountByTarget,
    km_clusterCount,
    km_clusterCountByTarget,
    km_iterations,
    km_iterationsByTarget,
    km_minTopicWeight,
    km_minTopicWeightByTarget,
    km_initStepsByTarget,
    km_initSteps,
    km_countTopClusterValues,
    lda_minTopicWeight,
    lda_minTopicWeightByTarget,
    lda_countTopClusterValues,
    rr_minEntryCountPerTarget,
    rr_maxDuplicateProportionPerTarget,
    cr_lowFrequencyPercentilToRemove,
    cr_highFrequencyPercentileToRemove,
    simpleLogName,
    cr_medianOffsetForLowFrequency,
    cr_stopwords,
    phraseAnalysisReportPath,
    phraseAnalysisDataSourceRootPath,
    phraseAnalysisFileNamePrefix,
    phraseAnalysisInputCsvUidColumnName,
    phraseAnalysisDataSourceFileName,
    phraseAnalysisTopTermCountPerClusterForAnalysis,
    phraseAnalysisAllTargetsColValue,
    //phraseAnalysisWhichPhraseAnalysisDataSets,
    phraseAnalysisSecondTierFileName,
    phraseCodingCleanOutputFile,
    phraseCodingTrainingDataFile,
    phraseCodingModelRootDir,
    phraseAnalysisOverwriteClusterCsv,
    analysisType,
    randomSubsetProportion,
    printTruncateValues,
    printStageName
  )
}
/***
  * Holds  configuration parameter values for a pipeline.
  * Sets default values upon instantiation
  * [[CustomPipelineConfiguration]] sets customized parameter values
  * [[CustomPipelineConfiguration]] passes in method sequences, including for subsequences
  *
  * [[PipelineConfiguration]].runPipeline starts the pipeline. It calls each method in sequence, passing the returned dataset from each method as the input parameter to the next.
  */
class PipelineConfiguration extends PipelineMethods  with SparkConfProviderWithStreaming with Serializable
{
  private[this] lazy val options = defaultValues
  private val methodMaps:mutable.HashMap[String, scala.collection.immutable.SortedMap[Int, Option[ikoda.sparse.RDDLabeledPoint] => Option[ikoda.sparse.RDDLabeledPoint]]]=new mutable.HashMap[String, scala.collection.immutable.SortedMap[Int, Option[ikoda.sparse.RDDLabeledPoint] => Option[ikoda.sparse.RDDLabeledPoint]]]()



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
    if (!PipelineConfiguration.names.contains(key))
    {
      throw new IKodaMLException(s"$key is an invalid option. Valid options are ${PipelineConfiguration.names.mkString(",")}")
    }
  }
  
  def showOptions():String =
  {
    options.toList.sorted.mkString("\n")
  }

  def methodsForPipe(key:String):Option[scala.collection.immutable.SortedMap[Int, Option[ikoda.sparse.RDDLabeledPoint] => Option[ikoda.sparse.RDDLabeledPoint]]]=
  {
        methodMaps.get(key)
  }
  
  def methodOrder(key:String, inMap: scala.collection.immutable.SortedMap[Int, Option[ikoda.sparse.RDDLabeledPoint] => Option[ikoda.sparse.RDDLabeledPoint]]): Unit =
  {
    methodMaps.put(key,inMap)
  }
  
  @throws(classOf[IKodaMLException])
  def runPipeline(key:String): Unit =
  {
    try
    {

      val methodOrderMap=methodMaps.get(key)
      methodOrderMap.isDefined match
        {
        case true=>
          var oSparse: Option[RDDLabeledPoint] = Some(new RDDLabeledPoint)
          methodOrderMap.get.foreach
          {
            method =>
              oSparse = method._2(oSparse)
          }
        case false => logger.warn("\n\nNo methods specified for "+key+"\n")
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
