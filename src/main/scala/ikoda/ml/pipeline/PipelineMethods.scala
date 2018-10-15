package ikoda.ml.pipeline

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.sparse.{ColumnHeadTuple, RDDLabeledPoint}
import ikoda.utilobjects._
import ikoda.utilobjects.IkodaPredicates.{intLT, stringContains, stringStartsWith}
import ikoda.utils.{CSVSpreadsheetCreator, Spreadsheet}

import scala.collection.mutable

/**
  * Each method in this trait encapsulate a specific data pipeline operation.
  *
  * A pipeline method always has the following signature:
  *
  * {{{(PipelineConfiguration => (Option[RDDLabeledPoint] => Option[RDDLabeledPoint]))}}}
  *
  * [[PipelineConfiguration]] holds any configuration parameters required for the method
  *
  * The Option[[RDDLabeledPoint]] input parameter is the data that will be processed.
  *
  * The Option[[RDDLabeledPoint]] return value can then become the input parameter for a subsequent method in the pipeline
  *
  * pconfig:[[PipelineConfiguration]] slightly violates functional programming principles. The configuration values can be changed as the pipeline prgresses.
  *
  * For example,  {{{pconfig.config(PipelineConfiguration.printStageName, "phrasePredicting")}}} may be changed in each method to signify the current stage when saving output to local files.
  *
  */
trait PipelineMethods extends Logging with SimpleLog with SparkConfProviderWithStreaming with UtilFunctions with StringPredicates with IntPredicates {

  type FTDataReductionProcess =
    (PipelineConfiguration => (Option[RDDLabeledPoint] => Option[RDDLabeledPoint]))
  @throws
  val phraseCodingSaveHumanCodesToCassandra: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {

      addLine("\n***phraseCoding***\n")
      val pc: PhraseCoding = new PhraseCoding(pconfig)
      if (osparse.isDefined) {
        osparse.get.repartition(1000)
        pconfig.config(PipelineConfiguration.printStageName, "phraseCodingSaveHumanCodesToCassandra")
        pc.phraseCodingSaveDataToCassandra(osparse)
      }
      else {
        logger.warn("No dataset provided. Aborting")
      }


      osparse


    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
        throw e
    }
  }
  @throws
  val phraseCoding: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {

      addLine("\n***phraseCoding "+pconfig.get(PipelineConfiguration.prefixForPrinting)+"***\n")
      val pc: PhraseCoding = new PhraseCoding(pconfig)
      if (osparse.isDefined) {
        osparse.get.repartition(1000)
        pc.phraseCodingModelGeneration(osparse.get)
        pconfig.config(PipelineConfiguration.printStageName, "phraseCoding")
      }
      else {
        logger.warn("No dataset provided. Aborting")
      }
      osparse
    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
        throw e
    }
  }
  @throws
  val phrasePredicting: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {

      addLine("\n***phrasePredicting***\n")
      val pc: PhraseCoding = new PhraseCoding(pconfig)
      if (osparse.isDefined) {
        osparse.get.repartition(1000)
        pc.phraseCodePrediction(osparse.get)
        pconfig.config(PipelineConfiguration.printStageName, "phrasePredicting")
      }
      else {
        logger.warn("No dataset provided. Aborting")
      }
      osparse
    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
        throw e
    }
  }
  @throws
  val phraseAnalysisKmeans: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {

      addLine("\n***phraseAnalysis Kmeans "+pconfig.get(PipelineConfiguration.prefixForPrinting)+"***\n")
      showMemoryUsage
      val phraseAnalysis: PhraseAnalysis = new PhraseAnalysis(pconfig, osparse)
      phraseAnalysis.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), "pa.log")

      addLine("\n***phraseAnalysis " + TermsDataReductionByClustering.kmeanscsv + "***\n")
      phraseAnalysis.phraseAnalysisFromReduction(TermsDataReductionByClustering.kmeanscsv)
      addLine("\n***phraseAnalysis " + TermsDataReductionByClustering.kmeansByTargetcsv + "***\n")
      phraseAnalysis.phraseAnalysisFromReduction(TermsDataReductionByClustering.kmeansByTargetcsv)
      addLine("\n***phraseAnalysis " + TermsDataReductionByClustering.secondTierLdacsv + "***\n")
      phraseAnalysis.phraseAnalysisFromReduction(TermsDataReductionByClustering.secondTierLdacsv)
      pconfig.config(PipelineConfiguration.printStageName, "phraseAnalysisKmeans")
      osparse
    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
        throw e
    }
  }
  @throws
  val phraseAnalysisLDA: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n***phraseAnalysis "+pconfig.get(PipelineConfiguration.prefixForPrinting)+"***\n")
      showMemoryUsage
      val phraseAnalysis: PhraseAnalysis = new PhraseAnalysis(pconfig, osparse)
      phraseAnalysis.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), "pa.log")

      addLine("\n***phraseAnalysis " + TermsDataReductionByClustering.ldacsv + "***\n")
      phraseAnalysis.phraseAnalysisFromReduction(TermsDataReductionByClustering.ldacsv)
      addLine("\n***phraseAnalysis " + TermsDataReductionByClustering.ldaByTargetcsv + "***\n")
      phraseAnalysis.phraseAnalysisFromReduction(TermsDataReductionByClustering.ldaByTargetcsv)
      addLine("\n***phraseAnalysis " + TermsDataReductionByClustering.secondTierLdacsv + "***\n")
      phraseAnalysis.phraseAnalysisFromReduction(TermsDataReductionByClustering.secondTierLdacsv)
      pconfig.config(PipelineConfiguration.printStageName, "phraseAnalysisLDA")
      osparse
    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
        throw e
    }
  }
  val initSimpleLog: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    initSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), pconfig.get(PipelineConfiguration.simpleLogName))
    clearLogFile()
    addLine(s"\n****Configuration****\n ${pconfig.showOptions}")
    osparse

  }
  @throws(classOf[IKodaMLException])
  val createEmptyRDDLabeledPoint: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      Some(new RDDLabeledPoint)


    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val loadDataAfterReducing: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n***loadDataAfterReducing***\n")
      addLine("This method reloads the dataset generated by reduceAndJoin which typically the first step in the pipeline. In other words, it is used to 'reset' the data, if, for example, you are creating multiple subsets, or if you don't need to run the entire pipeline from the raw input files.\n")

      showMemoryUsage
      val sparseOut = getOrThrow(RDDLabeledPoint.loadLibSvm(pconfig.get(PipelineConfiguration.reducedAndJoinedFileName), pconfig.get(PipelineConfiguration.pipelineOutputRoot)))
      showMemoryUsage
      addLine(s"Loaded Data ${PipelineConfiguration.reducedAndJoinedFileName} from ${pconfig.get(PipelineConfiguration.pipelineOutputRoot)}")
      addLine(s"${sparseOut.info}");

      Option(sparseOut)

    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val trimTargets: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n***trimTargets***\n")
      addLine(s"This will remove all rows from the dataset where the category count is lower than ${pconfig.get(PipelineConfiguration.rr_minEntryCountPerTarget)}")
      osparse.isDefined match
        {
        case true =>
          addLine("Before trimming:\n" + osparse.get.info)
          val dfrc = new TermsDataReductionByClustering(pconfig)


          val sparse: RDDLabeledPoint = osparse.get

          dfrc.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), pconfig.get(PipelineConfiguration.simpleLogName))


          val sparse1 = dfrc.trimTargets1(sparse, stringPredicates(), intPredicates(pconfig.getAsInt(PipelineConfiguration.rr_minEntryCountPerTarget)))
          val sparseOut = RDDLabeledPoint.resetColumnIndices(sparse1)

          sparseOut.isDefined match {
            case true =>
              pconfig.config(PipelineConfiguration.printStageName, "trimTargets")
              addLine("After trimming:\n" + sparseOut.get.info)
              sparseOut
            case _ => None
          }



        case false =>
          logger.warn("trimTargets: No data received ")
          None
      }



    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val mergeSimilarTargets: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n***PM mergeSimilarTargets"+pconfig.get(PipelineConfiguration.prefixForPrinting)+"***\n")
      addLine(s"This will merge similar targets such as accountancy and accounting")

      val dfrc = new TermsDataReductionByClustering(pconfig)

      val sparse: RDDLabeledPoint = osparse.get
      sparse.repartition(1000)


      dfrc.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), pconfig.get(PipelineConfiguration.simpleLogName))

      val sparse1 = dfrc.mergeSimilarTargets1(sparse, pconfig.get(PipelineConfiguration.mergeMapPropertiesFile))
      val sparseOut = RDDLabeledPoint.resetColumnIndices(sparse1)
      sparseOut.isDefined match {
        case true =>

          pconfig.config(PipelineConfiguration.printStageName, "mergeSimilarTargets")

          sparseOut
        case false => None

      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val printCountByTargets: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {

      val dfrc = new TermsDataReductionByClustering(pconfig)


      osparse.isDefined match {
        case true =>
          val sparse0: RDDLabeledPoint = osparse.get
          sparse0.repartition(1000)
          dfrc.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), pconfig.get(PipelineConfiguration.simpleLogName))
          dfrc.printCountByTarget(
            sparse0,
            pconfig.get(PipelineConfiguration.pipelineOutputRoot), s"${pconfig.get(PipelineConfiguration.prefixForPrinting)}CountRowsByTarget_${pconfig.get(PipelineConfiguration.keyspaceName)}_${pconfig.get(PipelineConfiguration.printStageName)}_${System.currentTimeMillis()}")
          Option(sparse0)

        case _ => logger.warn("printCountByTargets No data received.")
      }
      osparse

    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val evenProportionPerTarget: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n***evenProportionPerTarget***\n")
      addLine(s"This method removes rows from categories which are over-represented in the dataset. It does this by taking the median category count and removing rows at random from categories where the count is above the median\n")
      val dfrc = new TermsDataReductionByClustering(pconfig)


      val sparse0: RDDLabeledPoint = osparse.get


      addLine(s"Starting with ${sparse0.sparseData().count} rows")
      sparse0.repartition(50)
      val sparseEven = RDDLabeledPoint.evenProportionPerTarget(sparse0)

      sparseEven.isDefined match {
        case true =>
          addLine(s"After balancing. ${sparseEven.get.rowCountEstimate} rows (estimated)")
          val sparseOut = RDDLabeledPoint.resetColumnIndices(sparseEven.get)

          pconfig.config(PipelineConfiguration.printStageName, "evenProportionPerTarget")


          logger.debug("evenProportionPerTarget done")

          sparseOut
        case _ => None
      }
    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val removeHighAndLowFrequency: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {

      addLine("\n***removeHighAndLowFrequency***\n")
      addLine(s"This method removes columns with frequency in the top ${pconfig.get(PipelineConfiguration.cr_highFrequencyPercentileToRemove)} percentile")
      addLine(s"This method removes columns with frequency in the bottom less than median plus offset of ${pconfig.get(PipelineConfiguration.cr_medianOffsetForLowFrequency)} percentile")

      val tdr = new TermsDataReduction()
      tdr.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), pconfig.get(PipelineConfiguration.simpleLogName))
      val sparseReducedHigh = tdr.removeHighProportionFromSparse(osparse.get, pconfig)

      val sparse1 = tdr.removeLowFreqFromSparse(sparseReducedHigh._1, pconfig)
      val sparseOut = RDDLabeledPoint.resetColumnIndices(sparse1)
      pconfig.config(PipelineConfiguration.printStageName, "removeHighAndLowFrequency")
      printHashMapToCsv(sparseReducedHigh._2.toMap, "highFreqRemoved_"+pconfig.get(PipelineConfiguration.prefixForPrinting) + System.currentTimeMillis(), pconfig.get(PipelineConfiguration.pipelineOutputRoot))
      addLine("After column reduction:\n" + sparse1.info)
      sparseOut
    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val tfidf: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {

      addLine("\n***Normalize with TF-IDF***\n")

      pconfig.config(PipelineConfiguration.printStageName, "tfidf")
      val sparseTf = RDDLabeledPoint.termFrequencyNormalization(osparse)
      RDDLabeledPoint.inverseDocumentFrequency(sparseTf)


    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val reduceByTfIdf: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {

    addLine("\n***reduceByTfIdf***\n")

    addLine(s"This method converts the data to TFIDF format. ")
    addLine("It then removes the bottom 25% of columns by col sum.\n")
    osparse.isDefined match {
      case true => addLine("Before reduction " + osparse.get.info)
      case _ =>
    }

    val tdr: TermsDataReduction = new TermsDataReduction

    val sparseReducedo = tdr.reduceByTfIdf(osparse, pconfig.getAsInt(PipelineConfiguration.cr_lowFrequencyPercentilToRemove))
    sparseReducedo.isDefined match {
      case true => val sparseReducedTfidf = sparseReducedo.get
        pconfig.config(PipelineConfiguration.printStageName, "reduceByTfIdf")
        addLine("After TF-IDF at 25% cut " + sparseReducedTfidf.info())

        def extant: Set[Int] = osparse.get.columnHeads().map(cht => cht.numericLabel).toSet

        def reduced: Set[Int] = sparseReducedTfidf.columnHeads().map(cht => cht.numericLabel).toSet

        logger.debug("extant " + extant.size)
        logger.debug("reduced " + reduced.size)
        val colsToremove: Set[Int] = extant.diff(reduced)
        logger.debug("colsToremove " + colsToremove.size)
        val sparseOut = RDDLabeledPoint.removeColumnsDistributed(osparse.get, colsToremove)
        addLine(s"After removing by tfidf low col sum: \n${
          sparseOut.isDefined match {
            case true => sparseOut.get.info
            case false => "something bad happened"
          }
        }")
        sparseOut
      case _ =>
        osparse

    }
  }
  @throws(classOf[IKodaMLException])
  val reduceByClustering: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {

      addLine("\n***reduceByClustering***\n")
      addLine(s"This method runs LDA and k-means on the entire dataset. The process is recursive. It will remove terms that appear in more than ${pconfig.get(PipelineConfiguration.lda_termRepeatAcrossTopics)} topics and ${pconfig.get(PipelineConfiguration.km_termRepeatAcrossClusters)} clusters.")
      addLine("It will then rerun the analysis. Any term that appears in any iteration of the k-means or LDA analysis is kept. Terms (i.e., columns) that don't appear in any iteration of either anlysis are dropped from the dataset.\n")

      val dfrc = new TermsDataReductionByClustering(pconfig)

      val sparse0: RDDLabeledPoint = osparse.get
      dfrc.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), "TermsDataReductionByClustering.log")


      addLine("\n\n\n\n+++************K means analysis for entire dataset*************++++\n\n\n\n\n")
      val s1 = dfrc.kmeansProcessTermSelectionByCluster(sparse0.copy)
      addLine("\n\n\n\n+++************LDA analysis for entire dataset*************++++\n\n\n\n\n")
      val s2 = dfrc.ldaProcessTermSelectionByClusterAndTopic(sparse0.copy)


      val scombined: Set[String] = s1 ++ s2
      val scombinedIndices: Set[Int] = sparse0.columnIndexNameMap().filter(r => scombined.contains(r._2)).map(e => e._1).toSet


      require(scombined.size == scombinedIndices.size)

      val sparse1 = RDDLabeledPoint.removeColumnsDistributed(sparse0, scombinedIndices, false)
      sparse1.isDefined match {
        case true =>
          val sparseOut = RDDLabeledPoint.resetColumnIndices(sparse1.get)
          pconfig.config(PipelineConfiguration.printStageName, "reduceByClustering")
          addLine("\n\n\n--reduceByClustering done---\n")
          sparseOut
        case false =>
          logger.warn("Data empty after reduce by clustering")
          None
      }

    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val topTwoTargetsOnlySubset: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n****topTwoTargetsOnlySubset**\n")

      addLine(s"This method generates a smaller  subset (often for running unit system tests)")
      osparse.isDefined match {
        case true =>
          val sorted: scala.collection.immutable.TreeMap[Int, Double] = scala.collection.immutable.TreeMap(RDDLabeledPoint.countRowsByTargetCollected(osparse.get).map(_.swap).toArray: _*)
          val medianCount: Int = sorted.keySet.toSeq(sorted.size / 2)
          val fromMedian = sorted.filter(e => e._1 > medianCount)


          val keep: Seq[Double] = fromMedian.take(2).map(e => e._2).toSeq
          val toRemove: Seq[Double] = RDDLabeledPoint.countRowsByTargetCollected(osparse.get).filter {
            e => !keep.contains(e._1)
          }.map(e1 => e1._1).toSeq

          val sparse1 = RDDLabeledPoint.removeRowsByLabels(osparse.get, toRemove)

          pconfig.config(PipelineConfiguration.printStageName, "topTwoTargetsOnlySubset")
          addLine(sparse1.get.info)
          sparse1

        case false =>
          logger.warn("Failed to get top targets")
          None

      }
    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val randomSubset: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n****randomSubset**\n")

      addLine(s"This method generates a smaller random subset (often for running unit system tests)")

      osparse.isDefined match {
        case true =>


          val outo = RDDLabeledPoint.randomSubset(osparse.get, pconfig.getAsDouble(PipelineConfiguration.randomSubsetProportion))
          logger.info(outo.get.info)
          pconfig.config(PipelineConfiguration.printStageName, "randomSubset")
          outo
        case false =>
          logger.warn("No data  for random subset")
          None
      }
    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  @throws(classOf[IKodaMLException])
  val reduceByClusteringByTarget: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n***reduceByClusteringByTarget***\n")

      addLine(s"This method runs LDA and k-means for each target (i.e., category/label) in the dataset. Each target is treated as a standalone subset. The process is recursive. It will remove terms that appear in more than ${pconfig.get(PipelineConfiguration.lda_termRepeatAcrossTopics)} topics and ${pconfig.get(PipelineConfiguration.km_termRepeatAcrossClusters)} clusters.")
      addLine("It will then rerun the analysis. Any term that appears in any iteration of the k-means or LDA analysis for any of the targets is kept. Terms (i.e., columns) that don't appear in any iteration of either analysis for any target are dropped from the dataset.")

      val dfrc = new TermsDataReductionByClustering(pconfig)


      osparse.isDefined match {
        case true =>
          val sparse0: RDDLabeledPoint = osparse.get

          dfrc.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), "TermsDataReductionByClustering.log")

          addLine("\n\n\n\n+++************K means analysis By Category*************++++\n\n\n\n\n")
          val s1 = dfrc.kmeansProcessTermSelectionByClusterByTarget(sparse0)
          addLine("\n\n\n\n+++************LDA analysis By Category*************++++\n\n\n\n\n")
          val s2 = dfrc.ldaProcessTermSelectionByClusterAndTopicByTarget(sparse0)
          val scombined: Set[String] = s1 ++ s2
          val scombinedIndices = sparse0.columnIndexNameMap().filter(r => scombined.contains(r._2)).map(e => e._1).toSet
          require(scombined.size == scombinedIndices.size)

          val sparse1 = RDDLabeledPoint.removeColumnsDistributed(sparse0, scombinedIndices, false)

          val sparseOut = RDDLabeledPoint.resetColumnIndices(sparse1)
          pconfig.config(PipelineConfiguration.printStageName, "reduceByClusteringByTarget")


          sparseOut
        case false =>
          logger.warn("No data received for reduceByClusteringByTarget")
          None
      }

    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  val printLocally: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {


    RDDLabeledPoint.printSparseLocally(
      osparse,
      s"${pconfig.get(PipelineConfiguration.keyspaceName)}_${pconfig.get(PipelineConfiguration.printStageName)}_${pconfig.get(PipelineConfiguration.prefixForPrinting)}",
      pconfig.get(PipelineConfiguration.pipelineOutputRoot),
      Some(pconfig.getAsInt(PipelineConfiguration.printTruncateValues)))

    osparse
  }
  val runBySubset: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {


    def byTarget(pconfig: PipelineConfiguration, sparse0: RDDLabeledPoint, targets: Map[String, Double]): Unit = {
      targets.isEmpty match {
        case true =>
        case false =>
          val methodOrderMap = pconfig.methodsForPipe(PipelineConfiguration._subsetMethodMap)
          methodOrderMap.isDefined match {
            case true =>
              addLine("***By Subset: "+targets.head._1+"***")
              var oSparse: Option[RDDLabeledPoint] = RDDLabeledPoint.subsetByTarget(osparse.get, targets.head._2, targets.head._1)
              oSparse.isDefined match
                {
                case true =>
                  pconfig.config(PipelineConfiguration.prefixForPrinting,targets.head._1)
                  methodOrderMap.get.foreach {
                    method =>
                      oSparse = method._2(oSparse)
                  }
                case false =>
                  logger.warn("No data for "+targets.head._1)
              }



              byTarget(pconfig,sparse0,targets.tail)
            case false  => logger.warn("runBySubset No method map found for "+PipelineConfiguration._subsetMethodMap)
          }

      }
    }

    osparse.isDefined match {
      case false =>
        logger.warn("runBySubset No data received")
        None
      case true =>

        byTarget(pconfig,osparse.get,osparse.get.getTargets())
        osparse
    }
  }
  @throws(classOf[IKodaMLException])
  val duplicateAnalysis: FTDataReductionProcess = (pconfig: PipelineConfiguration) => (osparse: Option[RDDLabeledPoint]) => {
    try {
      addLine("\n***duplicateAnalysis***\n")

      val tdr = new TermsDataReduction
      tdr.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), pconfig.get(PipelineConfiguration.simpleLogName))
      osparse.isDefined match {
        case true =>
          val sparse0: RDDLabeledPoint = osparse.get

          if (pconfig.getAsDouble(PipelineConfiguration.rr_maxDuplicateProportionPerTarget) <= 0) {
            addLine("Before removing duplicates " + sparse0.info)
            val sparseOut = RDDLabeledPoint.removeDuplicateRows(sparse0)
            if (sparseOut.isDefined) {
              addLine("After removing duplicates " + sparseOut.get.info())
            }
            sparseOut
          }
          else {
            addLine("Before removing duplicates " + sparse0.info)
            val analysis: Seq[(Double, Int, Int, Double)] = tdr.analyzeDuplicates(osparse.get)
            val spreadsheetName: String = pconfig.get(PipelineConfiguration.keyspaceName) + "_duplicateAnalysis" + System.currentTimeMillis()
            Spreadsheet.getInstance().initCsvSpreadsheet(spreadsheetName, pconfig.get(PipelineConfiguration.pipelineOutputRoot))
            var uid = 0
            analysis.foreach {
              e =>
                Spreadsheet.getInstance().getCsvSpreadSheet(spreadsheetName).addCell(uid, "id", uid)
                Spreadsheet.getInstance().getCsvSpreadSheet(spreadsheetName).addCell(uid, "Target", e._1)
                Spreadsheet.getInstance().getCsvSpreadSheet(spreadsheetName).addCell(uid, "Entry Id", e._2)
                Spreadsheet.getInstance().getCsvSpreadSheet(spreadsheetName).addCell(uid, "Count", e._3)
                Spreadsheet.getInstance().getCsvSpreadSheet(spreadsheetName).addCell(uid, "Proportion of Total", e._4)
                uid = uid + 1
            }

            val toRemove: Seq[Int] = analysis.filter(e => e._4 > pconfig.getAsDouble(PipelineConfiguration.rr_maxDuplicateProportionPerTarget)).map(r => r._2)
            val sparse1 = RDDLabeledPoint.removeRowsByHashcode(sparse0, toRemove)
            Spreadsheet.getInstance().getCsvSpreadSheet(spreadsheetName).printCsvFinal()

            val sparseOut = RDDLabeledPoint.resetColumnIndices(sparse1)
            pconfig.config(PipelineConfiguration.printStageName, "duplicateAnalysis")

            if (sparseOut.isDefined) {
              addLine("After removing duplicates " + sparseOut.get.info)
            }
            addLine(s"See $spreadsheetName for duplicate analysis.")
            sparseOut

          }
        case false =>
          logger.warn("No data for duplicate analysis")
          None
      }


    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  def stringPredicates(): Seq[(String) => Boolean] = {
    val sw1 = stringContains("BLANK")(_)
    val sw2 = stringStartsWith("REL")(_)
    Seq(sw1, sw2)
  }

  def intPredicates(minCount: Int): Seq[(Int) => Boolean] = {
    val sw3 = intLT(minCount)(_)
    Seq(sw3)
  }

  @throws(classOf[IKodaMLException])
  def removeColumns(sparse1: RDDLabeledPoint, columnsToRemove: Set[Int]): RDDLabeledPoint = {
    try {
      val out = RDDLabeledPoint.removeColumnsDistributed(sparse1, columnsToRemove)
      out.isDefined match {
        case true => out.get
        case false => throw new IKodaMLException("failed to remove columns")
      }
    }
    catch {
      case e: Exception =>
        throw new IKodaMLException(e.getMessage, e)
    }
  }


}



