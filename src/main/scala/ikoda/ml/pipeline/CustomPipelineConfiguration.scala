package ikoda.ml.pipeline

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import ikoda.IKodaMLException
import ikoda.utilobjects.SparkConfProviderWithStreaming

import scala.collection.immutable.SortedMap

/**
  * A code based configuration mechanism for data analysis pipelines
  *
  * Each method specifies a pipeline.
  *
  * Each method sets configuration parameters in a [[PipelineConfiguration]] instance.
  *
  * It orders (curried) methods of type {{{(PipelineConfiguration => (Option[RDDLabeledPoint] => Option[RDDLabeledPoint]))}}} in a SortedMap. The methods are defined in [[PipelineMethods]]
  *
  * The [[PipelineConfiguration]] instance is passed to each method as the first parameter.
  *
  *  The data is passed in as the second parameter as each stage in the pipeline is run.
  *
  *
  */
object CustomPipelineConfiguration extends PipelineMethods with SparkConfProviderWithStreaming with CassandraPipelineMethods  with PipelineScratch
{
  val redditDocumentLevel = "redditDocumentLevel"
  val redditSentenceLevel = "redditSentenceLevel"
  val jobsDocumentLevel = "jobsDocumentLevel"
  val jobsSentenceLevel = "jobsSentenceLevel"
  val phraseAnalysisKmeansKey = "phraseAnalysisKmeans"
  val phraseAnalysisLDAKey = "phraseAnalysisLDA"
  val phraseCoding0 = "phraseCoding"
  val testRun = "testRun"

  val configTypes=Seq(redditDocumentLevel,redditSentenceLevel,jobsDocumentLevel,jobsSentenceLevel,phraseAnalysisKmeansKey,phraseAnalysisLDAKey,phraseCoding0,testRun)
  
  
  private def validateConfigType(s:String): Boolean=
  {
    configTypes.contains(s)
  }




  @throws(classOf[IKodaMLException])
  def apply(configType:String): PipelineConfiguration =
  {
    if(!validateConfigType(configType))
    {
        throw new IKodaMLException(s"Invalid configType: $configType Valid Types are ${configTypes.mkString(",")}")
    }

    configType match
    {
      case `jobsDocumentLevel`=> configJobsByDocument
      case `jobsSentenceLevel` => configJobsBySentence
      case `redditDocumentLevel`=>configRedditByPost
      case `redditSentenceLevel` => configRedditBySentence
      case `phraseAnalysisKmeansKey` => configPhraseAnalysisKmeans
      case `phraseAnalysisLDAKey` => configPhraseAnalysisLDA
      case `phraseCoding0` => configPhraseCoding()
      case `testRun` => configTestRun()
      case _ => new PipelineConfiguration
    }
  }
  
  private def configJobsByDocument():PipelineConfiguration=
  {
    val format = new SimpleDateFormat("y-M-d")


    val keyspaceUuid="612a88fd-ab4e-4ef9-ab19-ecb0fb6ebb4f"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bydocument")

    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")

    logger.info(s" Processing keyspaceName $keyspaceName  :  $keyspaceUuid")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()

    pconfig
      .config(PipelineConfiguration.analysisType,jobsDocumentLevel)
      .config(PipelineConfiguration.randomSubsetProportion,"0.30")
      .config(PipelineConfiguration.keyspaceName,keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")

      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,0)
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,100)
      .config(PipelineConfiguration.cr_medianOffsetForLowFrequency,-3)
      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClustering.log")

    val methodOrder=SortedMap(
      10->initSimpleLog(pconfig),
      20->loadFromCassandra(pconfig),
      21->printCountByTargets(pconfig),
      22->printLocally(pconfig),
      30->mergeSimilarTargets(pconfig),
      31->printCountByTargets(pconfig),
      32->printLocally(pconfig),
      40->trimTargets(pconfig),
      41->printCountByTargets(pconfig),
      42->printLocally(pconfig),
      50->duplicateAnalysis(pconfig),
      52->printLocally(pconfig),
      55 -> removeHighAndLowFrequency(pconfig),
      57->printLocally(pconfig),
      70->reduceByTfIdf(pconfig),
      71->printLocally(pconfig),
      90->tfidf(pconfig),
      91->printLocally(pconfig),
      92->loadFromCassandra(pconfig),
      93->trimTargets(pconfig),
      94->duplicateAnalysis(pconfig),
      99 -> runBySubset(pconfig)

    )

    val methodOrderSubset=SortedMap(
       10 -> removeHighAndLowFrequency(pconfig),
       30->printLocally(pconfig),
       60->tfidf(pconfig),
       90 -> printLocally(pconfig)
    )
    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)
    pconfig.methodOrder(PipelineConfiguration._subsetMethodMap,methodOrderSubset)
    pconfig
  }
  
  private def configPhraseAnalysisKmeans():PipelineConfiguration=
  {
    val format = new SimpleDateFormat("y-M-d")
    val phraseAnalysisSourceDir = ConfigFactory.load("scalaML").getString("scalaML.phraseAnalysis.sourceDir")

    val keyspaceUuid="612a88fd-ab4e-4ef9-ab19-ecb0fb6ebb4f"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bysentence")




    logger.info(s" Processing Root $phraseAnalysisSourceDir")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()

    pconfig
      .config(PipelineConfiguration.analysisType,phraseAnalysisKmeansKey)
      .config(PipelineConfiguration.randomSubsetProportion,0.30)

      .config(PipelineConfiguration.keyspaceName, keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot, phraseAnalysisSourceDir)

      .config(PipelineConfiguration.simpleLogName, "pa.log")
      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,750)
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,-1)

      .config(PipelineConfiguration.phraseAnalysisDataSourceRootPath,phraseAnalysisSourceDir)
      .config(PipelineConfiguration.phraseAnalysisInputCsvUidColumnName,"A_RowId")
      .config(PipelineConfiguration.phraseAnalysisReportPath,phraseAnalysisSourceDir)
      .config(PipelineConfiguration.phraseAnalysisTopTermCountPerClusterForAnalysis,"5")
      .config(PipelineConfiguration.phraseAnalysisSecondTierFileName,"secondTier.csv")

    
    val methodOrder=SortedMap(
      10 -> initSimpleLog(pconfig),
      20 -> loadFromCassandra(pconfig),
      30 -> mergeSimilarTargets(pconfig),
      40 -> trimTargets(pconfig),
      45 -> duplicateAnalysis(pconfig),
      46 -> removeHighAndLowFrequency(pconfig),

      ////////////DEBUG LINE//////////////
      // 5 -> randomSubset(pconfig),
      // 50 -> topTwoTargetsOnlySubset(pconfig),
      ///////////////////////////////////

      60 -> phraseAnalysisKmeans(pconfig)

    )

    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)

    pconfig

  }


  private def configPhraseCoding():PipelineConfiguration=
  {
    logger.info("configPhraseCoding")
    val format = new SimpleDateFormat("y-M-d")

    val phraseAnalysisSourceDir = ConfigFactory.load("scalaML").getString("scalaML.phraseAnalysis.codesSourceDir")
    val phraseAnalysisOutputCodeFile = ConfigFactory.load("scalaML").getString("scalaML.phraseAnalysis.codedOutputFile")
    val phraseAnalysisTrainingDataFile = ConfigFactory.load("scalaML").getString("scalaML.phraseAnalysis.trainingDataFile")
    val phraseAnalysisModelRoot = ConfigFactory.load("scalaML").getString("scalaML.modelsRoot.value")
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bysentence")
    val keyspaceUuid="19b96a84-6089-4293-8745-34c6b0e9fb19"



    logger.info(s" Processing Root $phraseAnalysisSourceDir")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()



    pconfig.config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.keyspaceName, keyspaceName)
      .config(PipelineConfiguration.pipelineOutputRoot, phraseAnalysisSourceDir)
      //.config(PipelineConfiguration.reducedAndJoinedFileName, "JOB_DATA_BY_DOCUMENT_ReducedAndJoined")
      .config(PipelineConfiguration.simpleLogName, "pcodinglog.log")
      .config(PipelineConfiguration.keyspaceUUID, "8a603632-d572-47f1-9ede-f3819c939ad6")
      .config(PipelineConfiguration.phraseCodingCleanOutputFile, phraseAnalysisOutputCodeFile)
      .config(PipelineConfiguration.phraseCodingTrainingDataFile, phraseAnalysisTrainingDataFile)
      .config(PipelineConfiguration.pipelineOutputRoot,phraseAnalysisSourceDir)
      .config(PipelineConfiguration.simpleLogName, "palog.log")
      .config(PipelineConfiguration.phraseAnalysisDataSourceRootPath,phraseAnalysisSourceDir)
      //.config(PipelineConfiguration.phraseAnalysisDataSourceFileName,phraseAnalysisSourceFile)
      .config(PipelineConfiguration.phraseAnalysisInputCsvUidColumnName,"uid")
     // .config(PipelineConfiguration.phraseAnalysisReportPath,phraseAnalysisSourceDir)
     // .config(PipelineConfiguration.phraseAnalysisTopTermCountPerClusterForAnalysis,"2")
      //.config(PipelineConfiguration.phraseAnalysisAllTargetsColValue,"ALL")
      //.config(PipelineConfiguration.phraseCodingModelRootDir,phraseAnalysisModelRoot)
      //.config(PipelineConfiguration.ldaCsvName,"lda.csv")
      //.config(PipelineConfiguration.ldaCsvNameByTarget,"ldaByTarget.csv")


    val methodOrder=SortedMap(
      1 ->initSimpleLog(pconfig),
      2 -> createEmptyRDDLabeledPoint(pconfig),
      3->phraseCodingSaveHumanCodesToCassandra(pconfig)
      //4->phrasePredicting(pconfig)

    )

    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)

    pconfig

  }






  private def configTestRun():PipelineConfiguration=
  {
    val format = new SimpleDateFormat("y-M-d")
    val keyspaceUuid="148a102c-f1f6-4129-a46d-b1559e80927e"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bysentence")
    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")

    logger.info(s" Processing keyspaceName $keyspaceName  :  $keyspaceUuid")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()

    pconfig
      .config(PipelineConfiguration.analysisType,jobsSentenceLevel)
      .config(PipelineConfiguration.randomSubsetProportion,"0.30")
      .config(PipelineConfiguration.keyspaceName,keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")
      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,-1)
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,1000)
      .config(PipelineConfiguration.cr_medianOffsetForLowFrequency,-1)

      /**Reduce By LDA*/

      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClustering.log")


    val methodOrder=SortedMap(
      10-> initSimpleLog(pconfig),
      20-> loadFromCassandra(pconfig),
      40-> trimTargets(pconfig),
      50 -> printLocally(pconfig),
      ////////////
      20-> loadFromCassandra(pconfig),
      58 -> runBySubset(pconfig)
    )

    val methodOrderSubset=SortedMap(
      40-> trimTargets(pconfig),
      50 -> printLocally(pconfig)


    )
    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)
    pconfig.methodOrder(PipelineConfiguration._subsetMethodMap,methodOrderSubset)
    pconfig

  }



  private def configPhraseAnalysisLDA():PipelineConfiguration=
  {
    val format = new SimpleDateFormat("y-M-d")
    val phraseAnalysisSourceDir = ConfigFactory.load("scalaML").getString("scalaML.phraseAnalysis.sourceDir")

    val keyspaceUuid="148a102c-f1f6-4129-a46d-b1559e80927e"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bysentence")




    logger.info(s" Processing Root $phraseAnalysisSourceDir")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()



    pconfig
      .config(PipelineConfiguration.analysisType,phraseAnalysisLDAKey)
      .config(PipelineConfiguration.randomSubsetProportion,0.30)

      .config(PipelineConfiguration.keyspaceName, keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot, phraseAnalysisSourceDir)

      .config(PipelineConfiguration.simpleLogName, "pa.log")
      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,1000)
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,-1)


      .config(PipelineConfiguration.phraseAnalysisDataSourceRootPath,phraseAnalysisSourceDir)
      .config(PipelineConfiguration.phraseAnalysisReportPath,phraseAnalysisSourceDir)
      .config(PipelineConfiguration.phraseAnalysisTopTermCountPerClusterForAnalysis,"5")




      val methodOrder=SortedMap(
        10 -> initSimpleLog(pconfig),
        20 -> loadFromCassandra(pconfig),
        30 -> mergeSimilarTargets(pconfig),
        40 -> trimTargets(pconfig),
        45 -> duplicateAnalysis(pconfig),
        45 -> duplicateAnalysis(pconfig),
        46 -> removeHighAndLowFrequency(pconfig),

        ////////////DEBUG LINE//////////////
        // 5 -> randomSubset(pconfig),
        // 50 -> topTwoTargetsOnlySubset(pconfig),
        ///////////////////////////////////



        60 -> phraseAnalysisLDA(pconfig)
        //3->trimTargets(pconfig)

    )

    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)

    pconfig

  }

  
  
  private def configRedditByPost():PipelineConfiguration=
  {
    val pconfig:PipelineConfiguration = new PipelineConfiguration()
    
    val format = new SimpleDateFormat("y-M-d")
    
    val rawDataRoot = ConfigFactory.load("scalaML").getString("scalaML.rawDataRoot.value")
    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")
    val rawDataFile = ConfigFactory.load("scalaML").getString("scalaML.dataFileNames.redditByDocument")
    
    pconfig
      .config(PipelineConfiguration.analysisType,redditDocumentLevel)
      .config(PipelineConfiguration.keyspaceName,"redditbypost")
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")
      .config(PipelineConfiguration.rawDataPath,s"${rawDataRoot}${File.separator}$rawDataFile")
      .config(PipelineConfiguration.reducedAndJoinedFileName,pconfig.get(PipelineConfiguration.keyspaceName)+"ReducedAndJoined")
      .config(PipelineConfiguration.targetColumn,"A_URI")
      .config(PipelineConfiguration.prefixForMerging,"BY_DOCUMENT")
      .config(PipelineConfiguration.uidCol,"A_RowId")
      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,0.001)
  
      .config(PipelineConfiguration.cr_highFrequencyPercentileToRemove,99.5)
      .config(PipelineConfiguration.cr_lowFrequencyPercentilToRemove,30)
  
      /**Reduce By LDA*/
      .config(PipelineConfiguration.lda_topicCount,20)
      .config(PipelineConfiguration.lda_termRepeatAcrossTopics,2)
      .config(PipelineConfiguration.lda_topicCountByTarget,12)
      .config(PipelineConfiguration.km_countTopClusterValues,12)
      .config(PipelineConfiguration.lda_minTopicWeight,0.02)
      .config(PipelineConfiguration.lda_minTopicWeightByTarget,0.005)
  
      /**Reduce By k-means*/
      .config(PipelineConfiguration.km_clusterCount,20)
      .config(PipelineConfiguration.km_termRepeatAcrossClusters,2)
      .config(PipelineConfiguration.km_clusterCountByTarget,12)
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,100)
  
  
      /**Output*/
      //.config(PipelineConfiguration.kmeansCsvName,"kmeans.csv")
      //.config(PipelineConfiguration.kmeansCsvNameByTarget,"kmeansByTarget.csv")
      //.config(PipelineConfiguration.ldaCsvName,"lda.csv")
      //.config(PipelineConfiguration.ldaCsvNameByTarget,"ldaByTarget.csv")
  
      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClustering.log")




    val methodOrder=SortedMap(
      1->initSimpleLog(pconfig)

    )

    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)

    pconfig

  }


  private def configRedditBySentence():PipelineConfiguration=
  {
    val pconfig:PipelineConfiguration = new PipelineConfiguration()
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.redditbysentence")
    val keyspaceUuid="98b2d41a-d38e-46ca-8465-e9e52f6b0da1"
    val format = new SimpleDateFormat("y-M-d")
    
    val rawDataRoot = ConfigFactory.load("scalaML").getString("scalaML.rawDataRoot.value")
    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")
    val rawDataFile = ConfigFactory.load("scalaML").getString("scalaML.dataFileNames.redditBySentence")
    
    pconfig
      .config(PipelineConfiguration.analysisType,redditSentenceLevel)
      .config(PipelineConfiguration.keyspaceName,keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")
      .config(PipelineConfiguration.rawDataPath,s"${rawDataRoot}${File.separator}$rawDataFile")

      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,0.00001)
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,1000)
      .config(PipelineConfiguration.cr_medianOffsetForLowFrequency,-1)

      /**Reduce By LDA*/
      .config(PipelineConfiguration.lda_topicCount,20)
      .config(PipelineConfiguration.lda_termRepeatAcrossTopics,2)
      .config(PipelineConfiguration.lda_topicCountByTarget,8)
      .config(PipelineConfiguration.lda_minTopicWeight,0.005)
      .config(PipelineConfiguration.lda_minTopicWeightByTarget,0.002)
      .config(PipelineConfiguration.lda_countTopClusterValues,10)

      /**Reduce By k-means*/
      .config(PipelineConfiguration.km_clusterCount,20)
      .config(PipelineConfiguration.km_countTopClusterValues,15)
      .config(PipelineConfiguration.km_termRepeatAcrossClusters,2)
      .config(PipelineConfiguration.km_clusterCountByTarget,12)
      .config(PipelineConfiguration.km_minTopicWeight,0.01)
      .config(PipelineConfiguration.km_minTopicWeightByTarget,0.04)
      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClusteringR.log")


    val methodOrder=SortedMap(
      10->initSimpleLog(pconfig),
      20->loadFromCassandra(pconfig),
      50->duplicateAnalysis(pconfig),

      55 -> removeHighAndLowFrequency(pconfig),

      ////////////DEBUG LINE//////////////
      //
      //60 -> topTwoTargetsOnlySubset(pconfig),
      //71 -> randomSubset(pconfig),
      ///////////////////////////////////
      70->evenProportionPerTarget(pconfig),
      80->reduceByClustering(pconfig),
      90->loadFromCassandra(pconfig),
      120->duplicateAnalysis(pconfig),

      ////////////DEBUG LINE//////////////
      // ,
      //140 -> topTwoTargetsOnlySubset(pconfig),
      //141 -> randomSubset(pconfig),
      ///////////////////////////////////
      155 -> removeHighAndLowFrequency(pconfig),

      160->reduceByClusteringByTarget(pconfig)
    )
    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)
    pconfig
  }
  
  
  private def configJobsBySentence():PipelineConfiguration=
  {
    val format = new SimpleDateFormat("y-M-d")
    val keyspaceUuid="148a102c-f1f6-4129-a46d-b1559e80927e"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bysentence")
    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")

    logger.info(s" Processing keyspaceName $keyspaceName  :  $keyspaceUuid")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()

    pconfig
      .config(PipelineConfiguration.analysisType,jobsSentenceLevel)
      .config(PipelineConfiguration.randomSubsetProportion,"0.30")
      .config(PipelineConfiguration.keyspaceName,keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")
      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,-1)
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,1000)
      .config(PipelineConfiguration.cr_medianOffsetForLowFrequency,-1)

      /**Reduce By LDA*/
      .config(PipelineConfiguration.lda_topicCount,20)
      .config(PipelineConfiguration.lda_termRepeatAcrossTopics,2)
      .config(PipelineConfiguration.lda_topicCountByTarget,8)
      .config(PipelineConfiguration.lda_minTopicWeight,0.005)
      .config(PipelineConfiguration.lda_minTopicWeightByTarget,0.002)
      .config(PipelineConfiguration.lda_countTopClusterValues,10)

      /**Reduce By k-means*/
      .config(PipelineConfiguration.km_clusterCount,20)
      .config(PipelineConfiguration.km_countTopClusterValues,15)
      .config(PipelineConfiguration.km_termRepeatAcrossClusters,2)
      .config(PipelineConfiguration.km_clusterCountByTarget,12)
      .config(PipelineConfiguration.km_minTopicWeight,0.01)
      .config(PipelineConfiguration.km_minTopicWeightByTarget,0.04)
      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClustering.log")


    val methodOrder=SortedMap(
      10->initSimpleLog(pconfig),
      20->loadFromCassandra(pconfig),
      30->mergeSimilarTargets(pconfig),
      40->trimTargets(pconfig),
      50->duplicateAnalysis(pconfig),
      55 -> removeHighAndLowFrequency(pconfig),

      ////////////DEBUG LINE//////////////
      //
      //60 -> topTwoTargetsOnlySubset(pconfig),
      //71 -> randomSubset(pconfig),
      ///////////////////////////////////
      70->evenProportionPerTarget(pconfig),
      80->reduceByClustering(pconfig),
      90->loadFromCassandra(pconfig),
      100->mergeSimilarTargets(pconfig),
      110->trimTargets(pconfig),

      120->duplicateAnalysis(pconfig),

      ////////////DEBUG LINE//////////////
      // ,
      //140 -> topTwoTargetsOnlySubset(pconfig),
      //141 -> randomSubset(pconfig),
      ///////////////////////////////////
      155 -> removeHighAndLowFrequency(pconfig),

      160->reduceByClusteringByTarget(pconfig)
    )
    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)

    pconfig

  }
  
  
  
  
}
