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
object CustomPipelineConfiguration extends PipelineMethods with SupplementPipelineMethods with CassandraPipelineMethods  with PipelineScratch
{
  val redditDocumentLevel = "redditDocumentLevel"
  val redditSentenceLevel = "redditSentenceLevel"
  val jobsDocumentLevel = "jobsDocumentLevel"
  val jobsSentenceLevel = "jobsSentenceLevel"
  val phraseAnalysisKmeansKey = "phraseAnalysisKmeans"
  val phraseAnalysisLDAKey = "phraseAnalysisLDA"
  val phraseCoding0 = "phraseCoding"
  val supplementOutput = "supplementOutput"
  val testRun = "testRun"

  val configTypes=Seq(redditDocumentLevel,redditSentenceLevel,jobsDocumentLevel,jobsSentenceLevel,phraseAnalysisKmeansKey,phraseAnalysisLDAKey,phraseCoding0,supplementOutput,testRun)
  
  
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
      case `supplementOutput` => configSupplementOutput()

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
      .config(PipelineConfiguration.cr_highFrequencyPercentileToRemove,99)
      .config(PipelineConfiguration.cr_lowFrequencyPercentilToRemove,75)
      .config(PipelineConfiguration.prefixForPrinting,"_")
      .config(PipelineConfiguration.cr_medianOffsetForLowFrequency,-3)
      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClustering.log")

    val methodOrder=SortedMap(
      10->initSimpleLog(pconfig),
      /**20->loadFromCassandra(pconfig),
      21->printCountByTargets(pconfig),
      22->printLocally(pconfig),
      30->mergeSimilarTargets(pconfig),
      40->trimTargets(pconfig),
      41->printCountByTargets(pconfig),
      50->duplicateAnalysis(pconfig),
      55 -> removeHighAndLowFrequency(pconfig),
      57->printLocally(pconfig),
      70->reduceByTfIdf(pconfig),
      71->printLocally(pconfig),
      90->tfidf(pconfig),
      91->printLocally(pconfig),**/

      12->loadFromCassandra(pconfig),
      //15 -> removeLowFrequencyByPercentile(pconfig),

      17 -> removeHighFrequency(pconfig),
      19 ->removeLowFrequencyByPercentile(pconfig),
      93->trimTargets(pconfig),
      94->duplicateAnalysis(pconfig),
      99 -> runBySubset(pconfig)

    )

    val methodOrderSubset=SortedMap(

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

    val keyspaceUuid="a7a24ebd-3194-4017-ab6b-0ce6dd08bdb6"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bysentence")




    logger.info(s" Processing Root $phraseAnalysisSourceDir")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()

    pconfig
      .config(PipelineConfiguration.analysisType,phraseAnalysisKmeansKey)

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
      46 -> removeHighFrequency(pconfig),
      48 -> removeLowFrequency(pconfig),
      ////////////DEBUG LINE//////////////
      // 5 -> randomSubset(pconfig),
      // 50 -> topTwoTargetsOnlySubset(pconfig),
      ///////////////////////////////////
      60 -> phraseAnalysisKmeans(pconfig),
      65 -> phraseAnalysisKmeansByTarget(pconfig),
      70 -> phraseAnalysisKmeans2ndTier(pconfig)
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
      3 -> phraseCodingSaveHumanCodesToCassandra(pconfig)
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
        46 -> removeHighFrequency(pconfig),
        48 -> removeLowFrequency(pconfig),

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
    val format = new SimpleDateFormat("y-M-d")
    val keyspaceUuid="e978e24a-4524-4edd-a579-df9ed3690afd"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.redditbydocument")
    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")
    logger.info(s" Processing keyspaceName $keyspaceName  :  $keyspaceUuid")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()



    pconfig
      .config(PipelineConfiguration.analysisType,jobsSentenceLevel)
      .config(PipelineConfiguration.keyspaceName,keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")
      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,-1)
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,1500)
      .config(PipelineConfiguration.cr_medianOffsetForLowFrequency,-1)
      .config(PipelineConfiguration.prefixForPrinting,"_")
      .config(PipelineConfiguration.cr_highFrequencyPercentileToRemove,99)
      .config(PipelineConfiguration.cr_lowFrequencyPercentilToRemove,0)


      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClustering.log")


    val methodOrder=SortedMap(
      10->initSimpleLog(pconfig),
      12->loadFromCassandra(pconfig),
      17 -> removeHighFrequency(pconfig),
      94->duplicateAnalysis(pconfig),
      99 -> runBySubset(pconfig)
    )

    val methodOrderSubset=SortedMap(
      30->printLocally(pconfig),
      60->tfidf(pconfig),
      90 -> printLocally(pconfig)
    )
    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)
    pconfig.methodOrder(PipelineConfiguration._subsetMethodMap,methodOrderSubset)

    pconfig

  }


  private def configRedditBySentence():PipelineConfiguration=
  {
    val format = new SimpleDateFormat("y-M-d")
    val keyspaceUuid="39dfd2ed-2928-4a0e-a03e-5e167deab307"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.redditbydocument")
    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")
    logger.info(s" Processing keyspaceName $keyspaceName  :  $keyspaceUuid")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()



    pconfig
      .config(PipelineConfiguration.analysisType,jobsSentenceLevel)
      .config(PipelineConfiguration.keyspaceName,keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")
      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,-1)
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,1500)
      .config(PipelineConfiguration.cr_medianOffsetForLowFrequency,-1)
      .config(PipelineConfiguration.prefixForPrinting,"_")
      .config(PipelineConfiguration.cr_highFrequencyPercentileToRemove,99)
      .config(PipelineConfiguration.cr_lowFrequencyPercentilToRemove,0)
      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClustering.log")

    val methodOrder=SortedMap(
      10->initSimpleLog(pconfig),
      12->loadFromCassandra(pconfig),
      17 -> removeHighFrequency(pconfig),
      94->duplicateAnalysis(pconfig),
      99 -> runBySubset(pconfig)
    )

    val methodOrderSubset=SortedMap(
      30->printLocally(pconfig),
      60->tfidf(pconfig),
      90 -> printLocally(pconfig)
    )
    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)
    pconfig.methodOrder(PipelineConfiguration._subsetMethodMap,methodOrderSubset)

    pconfig
  }
  
  
  private def configSupplementOutput():PipelineConfiguration=
  {
    val format = new SimpleDateFormat("y-M-d")
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bysentence")
    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")
    logger.info(s" Processing keyspaceName $keyspaceName ")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()

    pconfig
      .config(PipelineConfiguration.analysisType,supplementOutput)
      .config(PipelineConfiguration.keyspaceName,keyspaceName)
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")
      .config(PipelineConfiguration.prefixForPrinting,"_")
      .config(PipelineConfiguration.simpleLogName,"SupplementOutput.log")
      .config(PipelineConfiguration.pathToSpark,"/home/jake/environment/spark-2.3.2-bin-hadoop2.7/")



    val methodOrder=SortedMap(
      10 -> initSimpleLogDF(pconfig),
      20 -> loadSparseSupplement(pconfig),
      30 -> printSupplementByLabel(pconfig)

    )

    pconfig.methodOrderDataFrame(PipelineConfiguration._mainMethodMap,methodOrder)
    pconfig
  }


  private def configJobsBySentence():PipelineConfiguration=
  {
    val format = new SimpleDateFormat("y-M-d")
    val keyspaceUuid="a7a24ebd-3194-4017-ab6b-0ce6dd08bdb6"
    val keyspaceName = ConfigFactory.load("scalaML").getString("scalaML.keyspace.bysentence")
    val pipelineOutputRoot = ConfigFactory.load("scalaML").getString("scalaML.pipelineOutputRoot.value")
    logger.info(s" Processing keyspaceName $keyspaceName  :  $keyspaceUuid")
    val pconfig: PipelineConfiguration = new PipelineConfiguration()

    pconfig
      .config(PipelineConfiguration.analysisType,jobsSentenceLevel)
      .config(PipelineConfiguration.keyspaceName,keyspaceName)
      .config(PipelineConfiguration.keyspaceUUID,keyspaceUuid)
      .config(PipelineConfiguration.pipelineOutputRoot,s"${pipelineOutputRoot}${File.separator}${pconfig.get(PipelineConfiguration.keyspaceName)}${format.format(Calendar.getInstance().getTime())}_${System.currentTimeMillis()}")
      .config(PipelineConfiguration.mergeMapPropertiesFile, "/targetsMap.properties")
      .config(PipelineConfiguration.rr_maxDuplicateProportionPerTarget,-1)
      .config(PipelineConfiguration.rr_minEntryCountPerTarget,1500)
      .config(PipelineConfiguration.cr_medianOffsetForLowFrequency,-1)
      .config(PipelineConfiguration.prefixForPrinting,"_")
      .config(PipelineConfiguration.cr_highFrequencyPercentileToRemove,99)
      .config(PipelineConfiguration.cr_lowFrequencyPercentilToRemove,0)


      .config(PipelineConfiguration.simpleLogName,"TermsDataReductionByClustering.log")


    val methodOrder=SortedMap(
      10->initSimpleLog(pconfig),
      12->loadFromCassandra(pconfig),
      17 -> removeHighFrequency(pconfig),
      19 ->removeLowFrequencyByPercentile(pconfig),
      90 -> mergeSimilarTargets(pconfig),
      93->trimTargets(pconfig),
      94->duplicateAnalysis(pconfig),
      99 -> runBySubset(pconfig)

    )

    val methodOrderSubset=SortedMap(

      30->printLocally(pconfig),
      60->tfidf(pconfig),
      90 -> printLocally(pconfig)
    )
    pconfig.methodOrder(PipelineConfiguration._mainMethodMap,methodOrder)
    pconfig.methodOrder(PipelineConfiguration._subsetMethodMap,methodOrderSubset)

    pconfig
  }
  
}
