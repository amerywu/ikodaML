package ikoda.ml.pipeline

import java.net.URL
import java.util.Properties

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.sparse.{ColumnHeadTuple, RDDLabeledPoint}
import ikoda.utilobjects.{SimpleLog, SparkConfProviderWithStreaming, UtilFunctions}
import ikoda.utils.{IDGenerator, Spreadsheet}
import org.apache.spark.mllib.clustering.{LDA, LDAModel}
import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import ikoda.utilobjects.IkodaPredicates._

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object TermsDataReductionByClustering {

  val kmeanscsv="kmeans.csv"
  val kmeansByTargetcsv="kmeansByTarget.csv"
  val ldacsv="lda.csv"
  val ldaByTargetcsv="ldaByTarget.csv"
  val secondTierLdacsv="secondTierKmeans.csv"
}

/**
  * This class performs clustering functions (LDA and k means) on sparse data required by the analysis pipeline
  *
  *
  */
class TermsDataReductionByClustering(pconfig: PipelineConfiguration) extends Logging with SimpleLog with  UtilFunctions with SparkConfProviderWithStreaming {


  val termsToKeep: mutable.HashSet[String] = new mutable.HashSet[String]()
  val term: String = "Term"
  val target: String = "Target"
  val value: String = "Value"
  val iteration: String = "Iteration"
  val cluster: String = "Cluster/Topic"
  val atype: String = "AnalysisType"
  val lda: String = "LDA"
  val kmeans: String = "kmeans"

  var currentCsv=""



  def printCountByTarget(sparse0: Option[RDDLabeledPoint], outputDir: String, filePrefix: String):Unit=
  {
      sparse0.isDefined match
        {
        case true=> printCountByTarget(sparse0.get,outputDir,filePrefix)
        case _ => logger.warn("Nothing to print")
      }
  }

  @throws(classOf[IKodaMLException])
  def printCountByTarget(sparse0: RDDLabeledPoint, outputDir: String, filePrefix: String): Unit = {
    try {

      logger.debug("printCountByTarget")
      val toPrint = RDDLabeledPoint.countRowsByTargetCollected(sparse0)
      val csvName = s"${filePrefix}_printCountByTarget"

      Spreadsheet.getInstance().initCsvSpreadsheet(csvName, outputDir)
      var uid = 0
      toPrint.foreach {
        e =>
          Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(uid, "Target", e._1)
          Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(uid, "Target Name", sparse0.getTargetName(e._1))
          Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(uid, "Entry Count", e._2)
          uid = uid + 1

      }
      addLine(s"See $csvName for current row per target count.")
      Spreadsheet.getInstance().getCsvSpreadSheet(csvName).printCsvOverwrite(csvName)
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  def loadMergeMap(propertiesFileName: String): Option[Properties] = {
    logger.info("Loading " + propertiesFileName)
    try {
      val prop = new Properties()

      val urlo: Option[URL] = Option(getClass.getResource(propertiesFileName))
      if (urlo.isDefined) {
        logger.debug(urlo.get.getPath)
        val source = scala.io.Source.fromURL(urlo.get)
        prop.load(source.bufferedReader())
        logger.debug(prop.stringPropertyNames())
        logger.debug("loaded")
        Option(prop)
      }
      else {
        logger.debug("no url")
        None
      }


    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        None
    }
  }

  @throws
  def mergeSimilarTargets1(sparse0: RDDLabeledPoint, propertiesFileName: String): RDDLabeledPoint = {
    try {
      logger.debug(s"mergingTargets row count    ${sparse0.rowCountEstimate} ")

      addLine(s"Started with  ${sparse0.info()}")


      val mergeMapo = loadMergeMap(propertiesFileName)
      if (mergeMapo.isDefined) {

        val mergeMap: Properties = mergeMapo.get

        logger.debug("size: " + mergeMap.size())

        val mergeMapSeq: Seq[(String, String)] = mergeMap.asScala.toSeq

        val oldNew: Map[Double, Double] = mergeMapSeq.map {
          case (old_k, new_v) =>

            val kclean = s"#${old_k.replace('_', ' ')}"

            val oldTarget = kclean.toLowerCase()
            val newTarget = new_v.toLowerCase()
            logger.debug(s"$old_k ($oldTarget) | $new_v ($newTarget)")
            val oldTargetIdxo: Option[Double] = sparse0.getTargetId(oldTarget)
            val newTargetIdxo: Option[Double] = sparse0.getTargetId(newTarget)
            (oldTargetIdxo.isDefined && newTargetIdxo.isDefined) match {
              case true =>
                logger.debug(s"Changing $oldTarget to $newTarget")
                (oldTargetIdxo.get, newTargetIdxo.get)

              case false => (-1.0, -1.0)
            }

        }.toMap

        val sparse1 = RDDLabeledPoint.soMergeTargets(sparse0,oldNew)
        if (sparse1.isDefined) {
          addLine("After Merging: ")
          addLine(sparse1.get.info())

          sparse1.get
        }
        else {
          logger.warn("\n\nMerge Failed\n\n")
          sparse0
        }
      }
      else {
        logger.warn("\n\nNo Merge Properties File Found at " + propertiesFileName + "\n")
        sparse0
      }


    }
    catch {
      case e: Exception => throw new Exception(e)
    }

  }


  @throws
  def trimTargets1(sparse0: RDDLabeledPoint, stringCriteria: Seq[(String) => Boolean], intCriteria: Seq[(Int) => Boolean]): RDDLabeledPoint = {
    try {

      addLine("\n")
      addLine(s"Started with a row count of ${sparse0.rowCountEstimate}")
      addLine(s"Started with column count is ${sparse0.columnCount} ")
      val labelMap: Map[String, Int] = RDDLabeledPoint.countRowsByTargetCollected(sparse0).map {

        label =>
          (sparse0.getTargetName(label._1), label._2)
      }


      addLine(s"Initial target/label count    ${sparse0.targetCount()} ")


      val keySetPreds = or(stringCriteria)(_: String)
      val valuePreds = or(intCriteria)(_: Int)


      val keySetFilter = labelMap.keySet.filter(keySetPreds)
      val valuesFilter = labelMap.values.filter(valuePreds)

      val labelMapFiltered = labelMap.filter(label => keySetFilter.contains(label._1) || valuesFilter.toSeq.contains(label._2))


      logger.debug(s"labelMapFiltered size    ${labelMapFiltered.size} ")


      val toRemove: Seq[Double] = labelMapFiltered.map {
        label1 =>
          val targeto = sparse0.getTargetId(label1._1)
          if (targeto.isDefined) {
            targeto.get
          }
          else {
            logger.warn("trimTargets: NO target found for " + label1)
            -1.0
          }
      }.toSeq
      addLine(s"Removing rows for  ${toRemove.size} targets.")


      val aso = sparse0.getTargetId("any subject")
      val toRemoveFinal: Seq[Double] = aso.isDefined match {
        case true => toRemove ++ Seq(aso.get)
        case _ => toRemove


      }

      val sparseLabelsRemoved = getOrThrow(RDDLabeledPoint.removeRowsByLabels(sparse0,toRemoveFinal))


      addLine(s"Labels Removed:   ${sparseLabelsRemoved.info} ")


      //logger.debug(s"labelMap: ${labelMap.mkString("\n")}\n\n\n-----------------\n\n\n")
      //logger.debug(s"labelMapFiltered \n ${labelMapFiltered.mkString("\n")}")
      logger.debug(s"remaining ${sparseLabelsRemoved.getTargets().keySet.mkString("\n")}")

      sparseLabelsRemoved


    }
    catch {
      case e: Exception => throw new Exception(e)
    }

  }

  private def columnsNotInList(sparse0: RDDLabeledPoint): Set[String] = {
    val columnHeads: List[ColumnHeadTuple] = sparse0.columnHeads()
    columnHeads.filter(cht => !termsToKeep.contains(cht.stringLabel)).map(cht => cht.stringLabel).distinct.toSet

  }


  def setSimpleLog(path: String, fileName: String): Unit = {
    initSimpleLog(path, fileName)
  }

  @throws
  def tfidf(sparseIn:RDDLabeledPoint):RDDLabeledPoint=
  {
    try {
      val sparseTf = RDDLabeledPoint.termFrequencyNormalization(sparseIn)
      val sparseTfidf=RDDLabeledPoint.inverseDocumentFrequency(sparseTf)

      getOrThrow(sparseTfidf)
    }
    catch
      {
        case e:Exception => throw IKodaMLException(e.getMessage,e)
      }
  }

  @throws
  def kmeansProcessTermSelectionByCluster(sparseIn: RDDLabeledPoint): Set[String] = {


    logger.info("Starting processTermSelectionByClusterAndTopic")
    logger.debug(pconfig.showOptions())
    addLine("\nSAMPLE SIZE PER TARGET:")
    addLine("\nTarget -> Count")
    addLine(RDDLabeledPoint.countRowsByTargetCollected(sparseIn).mkString("\n"))


    val sparse0=tfidf(removeStopWords(sparseIn))
    currentCsv=TermsDataReductionByClustering.kmeanscsv


    val reportPath: String = pconfig.get(PipelineConfiguration.pipelineOutputRoot)
    val clusterCount: Int = pconfig.get(PipelineConfiguration.km_clusterCount).toInt
    val iterations:Int=pconfig.getAsInt(PipelineConfiguration.km_iterations)


    Spreadsheet.getInstance().initCsvSpreadsheet(currentCsv, reportPath)

    processKMeansTrimming(sparse0.copy, numClusters=clusterCount,iterations=iterations)

    val colsToRemove:Set[String] = columnsNotInList(sparse0)

    addLine("After running reduction, we  found " + colsToRemove.size + " columns to remove from the original total of " + sparse0.columnCount)

    printCsv(currentCsv)
    logger.info("Ending processTermSelectionByClusterAndTopic")
    colsToRemove
  }


  @throws
  def ldaProcessTermSelectionByClusterAndTopic(sparse0: RDDLabeledPoint): Set[String] = {
    logger.info("Starting processTermSelectionByClusterAndTopic")
    logger.debug(pconfig.showOptions())
    addLine("\nSAMPLE SIZE PER TARGET:")
    addLine("\nTarget -> Count")
    addLine(RDDLabeledPoint.countRowsByTargetCollected(sparse0).mkString("\n"))
    currentCsv=TermsDataReductionByClustering.ldacsv


    val reportPath: String = pconfig.get(PipelineConfiguration.pipelineOutputRoot)
    val clusterCount: Int = pconfig.get(PipelineConfiguration.km_clusterCount).toInt
    val topicCount: Int = pconfig.get(PipelineConfiguration.lda_topicCount).toInt

    Spreadsheet.getInstance().initCsvSpreadsheet(currentCsv, reportPath)

    processLDATrimming(sparse0.copy, topicCount)

    val colsToRemove = columnsNotInList(sparse0)

    addLine("After running reduction, we are found " + colsToRemove.size + " columns to remove from the original total of " + sparse0.columnCount)


    printCsv(currentCsv)

    logger.info("Ending processTermSelectionByClusterAndTopic")
    colsToRemove
  }



  @throws
  def ldaProcessTermSelectionNoRecursion(sparse0: RDDLabeledPoint, subsetName:String): RDDLabeledPoint = {


    logger.info("Starting ldaProcessTermSelectionNoRecursion")
    addLine("\nSAMPLE SIZE PER TARGET:")
    addLine("\nTarget -> Count")
    addLine(RDDLabeledPoint.countRowsByTargetCollected(sparse0).mkString("\n"))
    currentCsv=TermsDataReductionByClustering.secondTierLdacsv


    val reportPath: String = pconfig.get(PipelineConfiguration.pipelineOutputRoot)
    val clusterCount: Int = pconfig.getAsInt(PipelineConfiguration.km_clusterCount)
    val topicCount: Int = pconfig.getAsInt(PipelineConfiguration.lda_topicCount)




    Spreadsheet.getInstance().initCsvSpreadsheet(currentCsv, reportPath)


    doLDACycleNoRecursion(topicCount, sparse0, subsetName, pconfig.getAsDouble(PipelineConfiguration.lda_minTopicWeight))


    printCsv(currentCsv)
    Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).clearData()


    sparse0
  }


  @throws
  def kmeansProcessTermSelectionNoRecursion(sparseIn: RDDLabeledPoint, subsetName:String): RDDLabeledPoint = {


    logger.info("Starting kmeansProcessTermSelectionNoRecursion")
    addLine("\nSAMPLE SIZE PER TARGET:")
    addLine("\nTarget -> Count")
    addLine(RDDLabeledPoint.countRowsByTargetCollected(sparseIn).mkString("\n"))
    val sparse0=tfidf(removeStopWords(sparseIn))
    currentCsv=TermsDataReductionByClustering.secondTierLdacsv


    val reportPath: String = pconfig.get(PipelineConfiguration.pipelineOutputRoot)
    val clusterCount: Int = pconfig.getAsInt(PipelineConfiguration.km_clusterCount)
    val steps:Int = pconfig.getAsInt(PipelineConfiguration.km_initSteps)





    Spreadsheet.getInstance().initCsvSpreadsheet(currentCsv, reportPath)



    doKmeansCycleNoRecursion(sparse0,
      subsetName,
      numClusters=clusterCount,
      initSteps  =steps
    )


    printCsv(currentCsv)
    Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).clearData()


    sparse0
  }

  private def printCsv(name: String): Unit = {
    try {
      pconfig.getAsBoolean(PipelineConfiguration.phraseAnalysisOverwriteClusterCsv) match {
        case true => Spreadsheet.getInstance().getCsvSpreadSheet(name).printCsvOverwrite(name)
        case _ =>
          if (Spreadsheet.getInstance().getCsvSpreadSheet(name).rowCount() > 0) {
            Spreadsheet.getInstance().getCsvSpreadSheet(name).printCsvAppend(name)
          }
      }
    }
    catch {
      case e: Exception => throw  IKodaMLException(e.getMessage, e)
    }
  }


  private def removeStopWords(sparse0:RDDLabeledPoint): RDDLabeledPoint =
  {
    try
    {
      pconfig.getAsOption(PipelineConfiguration.cr_stopwords).isDefined match
        {
        case false =>
          addLine("No stop words registered")
          sparse0
        case true=> val stopWordSeq:Seq[String] = pconfig.get(PipelineConfiguration.cr_stopwords).split(",")
          val columnIndicesSeq:Seq[Int]= stopWordSeq.map(w=> sparse0.getColumnIndex(w,true)).filter(idx => idx >= 0)
          addLine(s"Removing ${columnIndicesSeq.length} stop words (i.e., columns)")
          addLine(s"Stop words are: ${stopWordSeq.mkString(" | ")}\n")
          getOrThrow(RDDLabeledPoint.removeColumnsDistributed(sparse0,columnIndicesSeq.toSet))
      }
    }
    catch
      {
        case e:Exception => logger.warn(s"Failed to remove stop words ${e.getMessage}",e)
          addLine(s"Failed to remove stop words ${e.getMessage}")
          sparse0
      }
  }

  @throws
  def kmeansProcessTermSelectionByClusterByTarget(sparseIn: RDDLabeledPoint): Set[String] = {

    logger.info("Starting kmeansProcessTermSelectionByClusterByTarget")
    addLine("SAMPLE SIZE PER TARGET")
    addLine("\nTarget -> Count")
    addLine(RDDLabeledPoint.countRowsByTargetCollected(sparseIn).mkString("\n"))
    currentCsv=TermsDataReductionByClustering.kmeansByTargetcsv
    val sparse0=tfidf(removeStopWords(sparseIn))

    val reportPath: String = pconfig.get(PipelineConfiguration.pipelineOutputRoot)
    val clusterCount: Int = pconfig.getAsInt(PipelineConfiguration.km_clusterCountByTarget)
    val iterations: Int = pconfig.getAsInt(PipelineConfiguration.km_iterationsByTarget)

    //sparse0.validateColumnCount
    Spreadsheet.getInstance().initCsvSpreadsheet(currentCsv, reportPath)



    processKMeansTrimmingByTarget(sparse0, clusterCount,iterations)

    val colsToRemove = columnsNotInList(sparse0)

    addLine("After running reduction, we  found " + colsToRemove.size + " columns to remove from the original total of " + sparse0.columnCount)


    printCsv(currentCsv)
    logger.info("Completed kmeansProcessTermSelectionByClusterByTarget")
    colsToRemove
  }



  @throws
  def ldaProcessTermSelectionByClusterAndTopicByTarget(sparse0: RDDLabeledPoint): Set[String] = {

    logger.info("Starting ldaProcessTermSelectionByClusterAndTopicByTarget")
    addLine("SAMPLE SIZE PER TARGET")
    addLine("\nTarget -> Count")
    addLine(RDDLabeledPoint.countRowsByTargetCollected(sparse0).mkString("\n"))
    currentCsv=TermsDataReductionByClustering.ldaByTargetcsv


    val reportPath: String = pconfig.get(PipelineConfiguration.pipelineOutputRoot)

    val topicCount: Int = pconfig.getAsInt(PipelineConfiguration.lda_topicCountByTarget)


    Spreadsheet.getInstance().initCsvSpreadsheet(currentCsv, reportPath)



    processLDATrimmingByTarget(sparse0, topicCount)
    val colsToRemove = columnsNotInList(sparse0)

    addLine("After running reduction, we found " + colsToRemove.size + " columns to remove from the original total of " + sparse0.columnCount)


    printCsv(currentCsv)
    logger.info("Completed ldaProcessTermSelectionByClusterAndTopicByTarget")
    colsToRemove
  }


  @throws
  private def processKMeansTrimming(sparse0: RDDLabeledPoint, numClusters: Int, iterations:Int): Unit = {
    try {

      logger.info("\n\t\tKmeans CYCLE\n")

      addLine("***********KMEANS ANALYSIS***********\n\n")


      addLine("----------KMEANS ANALYSIS FOR FULL DATASET------------")

      doKmeansCycle(
        sparse0.copy(),
        pconfig.get(PipelineConfiguration.phraseAnalysisAllTargetsColValue),
        iterations=iterations,
        numClusters = numClusters,
        initSteps =  pconfig.getAsInt(PipelineConfiguration.km_initSteps)

      )

    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new Exception(e)
    }
  }

  @throws
  private def processKMeansTrimmingByTarget(sparse0: RDDLabeledPoint, clusterCount: Int, iterations:Int): Unit = {
    try {

      logger.info("\n\t\tKmeans CYCLE\n")

      addLine("***********KMEANS ANALYSIS BY TARGET***********\n\n")

      val targets: Map[String, Double] = sparse0.getTargets()

      addLine(s"There are ${targets.size} targets. ")

      var count=1
      targets.foreach {
        case (stringValue, numericValue) => {
          logger.info(s"\n\n\n\nNEW TARGET $stringValue\n\n\n")

          addLine(s"*----------K MEANS ANALYSIS FOR $stringValue ($count out of ${targets.size} targets)------------")
          count = count+1
          val sparseSubsetO: Option[RDDLabeledPoint] = RDDLabeledPoint.subsetByTarget(sparse0,numericValue, stringValue)

          if (sparseSubsetO.isDefined) {
            //Try(sparseSubsetO.get.printSparseLocally("stringValue",options.get(TermsDataReductionByClustering.optionReportPath).get))
            doKmeansCycle(
              sparseSubsetO.get,
              stringValue,
              iterations,
              clusterCount,
              initSteps =  pconfig.getAsInt(PipelineConfiguration.km_initStepsByTarget)

            )
          }
        }
      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new Exception(e)
    }
  }


  @throws
  private def doLDA(sparse0: RDDLabeledPoint, topicCount: Int = 20, proportion: Double = 0.99): Option[(LDAModel,Map[Int,String])] = {
    if (proportion > 1) {
      throw new Exception("Proportion must be a value less than  1")
    }
    try {
      logger.info("DO LDA");
      val sparse1:RDDLabeledPoint=getOrThrow(RDDLabeledPoint.resetColumnIndices(sparse0))
      logger.debug("Max col index: "+sparse1.columnMaxIndex)
      logger.debug(sparse1.info)
      sparse1.rowCountEstimate > 10 match {
        case true =>

          val data = sparse1.convertToMLLibPackageRDD()
          val splits = data.randomSplit(Array(proportion, 1 - proportion))
          val (trainingData, testData) = (splits(0), splits(1))


          val mappedRDD: RDD[(Long, org.apache.spark.mllib.linalg.Vector)] = trainingData.zipWithIndex().map {

            case (row, idx) =>

              new Tuple2(idx, row.features.toSparse)
          }


          logger.debug("Running Model")

          val lda = new LDA()
          lda.setOptimizer("online")
          val model: LDAModel = lda.setK(topicCount).run(mappedRDD)
          Option((model,sparse1.columnIndexNameMap()))
        case _ => None
      }
    }
    catch {
      case e: Exception => addLine(e.getMessage)
        logger.error(e.getMessage, e)
        Try(RDDLabeledPoint.printSparseLocally(sparse0,"ldaError", pconfig.get(PipelineConfiguration.pipelineOutputRoot)))
        None
    }
  }

  def addItemToClusterCSV(columnMap:Map[Int,String],itruid: String,trm:Int,weight:Double,targetName:String,currentIteration:Int,clusterIdx:Int, intype:String): Unit =
  {
    val uid = IDGenerator.getInstance().nextID()
    val cuid = itruid + "_" + clusterIdx

    Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).addCell(uid.toString, term, columnMap.getOrElse(trm.toInt,"Column Not Found"))

    Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).addCell(uid, value, weight)
    Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).addCell(uid, target, targetName)
    Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).addCell(uid, iteration, currentIteration)
    Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).addCell(uid, cluster, cuid)
    Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).addCell(uid, atype, intype)
  }


  private def reportLDA(ldaModel: LDAModel, columnMap:Map[Int,String], targetName: String, currentIteration: Int, minWeight: Double): mutable.HashMap[String, Int] = {


    addLine("****Reporting LDA Iteration " + currentIteration + " for "+targetName+"****\n")
    // Output topics. Each is a distribution over words (matching word count vectors)
    addLine("\n\nLearned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):\n")

    ///logger.info(s"k is ${ldaModel.k}")

    val termToTopicCount: mutable.HashMap[String, Int] = new mutable.HashMap()
    val topics = ldaModel.topicsMatrix
    val itruid: String = IDGenerator.getInstance().nextUUID()
    val countValues: Int = pconfig.getAsInt(PipelineConfiguration.lda_countTopClusterValues)
    addLine("Iteration Id="+itruid)
    val topicDescriptions: Array[(Array[Int], Array[Double])] = ldaModel.describeTopics(maxTermsPerTopic = 60)

    logger.info(s"LDA created ${topicDescriptions.size} topic descriptions")


    var i = 0
    topicDescriptions.foreach {
      case (terms, termWeights) =>

        addLine(s"\nLDA weights max: ${termWeights.max}")
        addLine(s"LDA weights median: ${termWeights(termWeights.length / 2)}")
        addLine(s"LDA weights avg: ${termWeights.sum / termWeights.length}")
        addLine(s"LDA weights threshold: ${minWeight}")
        addLine("\n")
        var currentCountTerms = 0;
        terms.zip(termWeights).foreach {
          case (trm, weight) =>

            //if (weight > minWeight)
            if(currentCountTerms < countValues)
            {

              addLine((f"${columnMap.getOrElse(trm.toInt,"Column Not Found")} [$weight%1.4f]   \t"))

              val topicinclusioncountO = termToTopicCount.get(columnMap.getOrElse(trm.toInt,"Column Not Found"))
              addItemToClusterCSV(columnMap,itruid,trm,weight,targetName,currentIteration,i,"lda")

              if (!topicinclusioncountO.isDefined) {
                termToTopicCount.put(columnMap.getOrElse(trm.toInt,"Column Not Found"), 0)
              }
              else {
                termToTopicCount.put(columnMap.getOrElse(trm.toInt,"Column Not Found"), (topicinclusioncountO.get + 1))
              }
            }
            currentCountTerms = currentCountTerms +1
        }
        i = i + 1
    }

    addLine("Generated " + Spreadsheet.getInstance().getCsvSpreadSheet(currentCsv).rowCount() + " rows in " + currentCsv)
    termToTopicCount

  }


  private def doLDACycleNoRecursion(topicCount: Int, sparse0: RDDLabeledPoint, target: String, minWeight: Double, currentIteration: Int = 0): Unit = {

    addLine(s"\n\n\n\n\t\t*************LDA for $target ***********\n")
    addLine(sparse0.info)
    sparse0.sparseData().cache()
    val oldaModel: Option[(LDAModel,Map[Int,String])] = doLDA(sparse0, topicCount)
    if (oldaModel.isDefined) {
      reportLDA(oldaModel.get._1, oldaModel.get._2, target, currentIteration, minWeight)

    }
  }

  private def doKmeansCycleNoRecursion(sparse0: RDDLabeledPoint,
                            targetName: String,
                            numClusters: Int = 10,
                            proportion: Double = 0.9,
                            initSteps:Int = 5,
                            currentIteration: Int = 0): Unit = {

    addLine(s"\n\n\n\n\t\t************* Kmeans for $target ***********\n")
    addLine(sparse0.info)
    sparse0.sparseData().cache()
    val kModelo: Option[(KMeansModel,Map[Int,String])] = doKmeans(sparse0, numClusters = numClusters, initSteps=initSteps,proportion = 0.9)
    if (kModelo.isDefined) {
      reportKmeans(kModelo.get._1, kModelo.get._2, target, 1)
    }
  }






  private def doLDACycle(topicCount: Int, sparse0: RDDLabeledPoint, target: String, minWeight: Double, currentIteration: Int = 0): Unit = {

    addLine(s"\n\n\n\n\t\t*************LDA CYCLE for $target ***********\n")
    addLine(sparse0.info)
    sparse0.sparseData().cache()
    val oldaModel: Option[(LDAModel,Map[Int,String])] = doLDA(sparse0, topicCount)
    if (oldaModel.isDefined) {
      val termToTopicCount: mutable.HashMap[String, Int] = reportLDA(oldaModel.get._1, oldaModel.get._2, target, currentIteration, minWeight)
      termsToKeep ++= termToTopicCount.keySet
      val colsToRemove: List[String] = iterativeTrimming(termToTopicCount, pconfig.getAsInt(PipelineConfiguration.lda_termRepeatAcrossTopics), sparse0)
      logger.debug(s"Removing columns ${colsToRemove.mkString("\n")}")
      if (colsToRemove.size > 1) {

        val idxSet:Set[Int]= colsToRemove.map(s => sparse0.getColumnIndex(s)).toSet
        val sparse1 = getOrThrow(RDDLabeledPoint.removeColumnsDistributed(sparse0,idxSet))

        addLine(s"Removing ${colsToRemove.mkString(",")}")
        doLDACycle(topicCount, sparse1, target, minWeight, currentIteration + 1)
      }
    }
  }


  private def processLDATrimming(sparse0: RDDLabeledPoint, topicCount: Int): Unit = {
    try {
      addLine("***********LDA ANALYSIS***********\n\n")


      addLine("*----------LDA ANALYSIS FOR FULL DATASET------------")
      doLDACycle(topicCount, sparse0.copy(), pconfig.get(PipelineConfiguration.phraseAnalysisAllTargetsColValue), pconfig.getAsDouble(PipelineConfiguration.lda_minTopicWeight))

    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new Exception(e)
    }
  }

  private def processLDATrimmingByTarget(sparse0: RDDLabeledPoint, topicCount: Int): Unit = {
    try {
      addLine("***********LDA ANALYSIS***********\n\n")

      val targets: Map[String, Double] = sparse0.getTargets()
      var count=1
      addLine(s"There are ${targets.size} targets. ")
      targets.foreach {
        case (stringValue, numericValue) => {
          logger.info(s"\n\n\n\nNEW TARGET $stringValue\n\n\n")
          logger.info(s"")
          addLine(s"*----------LDA ANALYSIS FOR $stringValue ($count out of ${targets.size} targets)------------")

          count = count+1
          val sparseSubsetO: Option[RDDLabeledPoint] = RDDLabeledPoint.subsetByTarget(sparse0,numericValue, stringValue)
          if (sparseSubsetO.isDefined)
          {
            doLDACycle((topicCount), sparseSubsetO.get, stringValue, pconfig.getAsDouble(PipelineConfiguration.lda_minTopicWeight))
          }
        }
      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new Exception(e)
    }
  }


  private def reportKmeans(kModel: KMeansModel, colMap:Map[Int,String], targetName: String, currentIteration: Int): mutable.HashMap[String, Int] =
  {
    addLine("****Reporting KMeans Iteration " + currentIteration + " for "+targetName+"****\n")
    val termToTopicCount: mutable.HashMap[String, Int] = new mutable.HashMap()
    val itruid: String = IDGenerator.getInstance().nextUUID()
    var i = 0
    val countValues: Int = pconfig.getAsInt(PipelineConfiguration.km_countTopClusterValues)



    addLine(s"Kmeans created ${kModel.clusterCenters.toSeq.size} clusters\n")
    kModel.clusterCenters.toSeq.foreach {
      clusterCenter =>
        val mapByValue: ListMap[Double, Int] = ListMap(clusterCenter.toSparse.indices.map {
          colIdx =>
            new Tuple2(clusterCenter(colIdx), colIdx)
        }.toSeq.sortWith(_._1 > _._1): _*)


        addLine("Kmeans weights: Max value " + mapByValue.keySet.toSeq.max)
        addLine("Kmeans weights: Avg value " + mapByValue.keySet.toSeq.sum / mapByValue.size)
        addLine("Kmeans weights: Median value " + mapByValue.keySet.toSeq(mapByValue.size / 2))
        var currentCountValues = 0;

        mapByValue.foreach {
          case (kvalue, colId) =>

            if (currentCountValues <= countValues) {
              val sb: StringBuilder = new StringBuilder
              val uid = IDGenerator.getInstance().nextID()
              val cuid = itruid + "_" + i
              sb.append(colMap.getOrElse(colId,"Column Not Found"))
              sb.append(" - ")
              sb.append(kvalue)

              addItemToClusterCSV(colMap,itruid,colId,kvalue,targetName,currentIteration,i,"kmeans")


              val clusterinclusioncountO = termToTopicCount.get(colMap.getOrElse(colId,"Column Not Found"))

              if (!clusterinclusioncountO.isDefined) {
                termToTopicCount.put(colMap.getOrElse(colId,"Column Not Found"), 0)
              }
              else {
                termToTopicCount.put(colMap.getOrElse(colId,"Column Not Found"), (clusterinclusioncountO.get + 1))
              }
              addLine(sb.toString())
            }
            currentCountValues = currentCountValues + 1

        }
        addLine("\n\t---\t\n")
        i = i + 1
    }

    termToTopicCount
  }


  private def doKmeansCycle(sparse0: RDDLabeledPoint,
                            targetName: String,
                            iterations:Int=20,
                            numClusters: Int = 10,
                            proportion: Double = 0.9,
                            initSteps:Int = 5,
                            currentIteration: Int = 0): Unit = {

    addLine(s"\n\n\n\t\t****Kmeans CYCLE for $targetName *****  \n")
    addLine(sparse0.info)
    //sparse0.validateColumnCount
    sparse0.sparseData().cache()
    val kModelo: Option[(KMeansModel,Map[Int,String])] = doKmeans(sparse0, numClusters = numClusters, iterations=iterations, proportion = 0.9, initSteps=initSteps)

    if (kModelo.isDefined) {
      val termToTopicCount: mutable.HashMap[String, Int] = reportKmeans(kModelo.get._1, kModelo.get._2, targetName, currentIteration)
      termsToKeep ++= termToTopicCount.keySet
      val colsToRemove: List[String] = iterativeTrimming(
        termToTopicCount, pconfig.getAsInt(PipelineConfiguration.km_termRepeatAcrossClusters), sparse0
      )

      if (colsToRemove.size > 0) {

        logger.debug("Before removing:"+sparse0.info())
        logger.debug("Max idx:"+sparse0.columnMaxIndex)
        logger.debug("cols to remove:"+colsToRemove.mkString(" | "))

        val idxSet:Set[Int]= colsToRemove.map(s => sparse0.getColumnIndex(s)).toSet
        logger.debug("indices to remove:"+idxSet.mkString(" , "))
        val sparse1 = getOrThrow(RDDLabeledPoint.removeColumnsDistributed(sparse0,idxSet))
        addLine(s"Removed columns for ${colsToRemove.mkString(",")}")
        doKmeansCycle(
          sparse1,
          targetName,
          iterations=iterations,
          numClusters=numClusters,
          initSteps =  pconfig.getAsInt(PipelineConfiguration.km_initStepsByTarget),
          currentIteration=currentIteration +1

        )
      }
    }
  }

  private def iterativeTrimming(termToTopicCount: mutable.HashMap[String, Int], repeatTopicThreshold: Int, sparse0: RDDLabeledPoint): List[String] = {
    val array: ArrayBuffer[String] = new ArrayBuffer[String]()
    termToTopicCount.map {
      tuple =>
        if (tuple._2 > repeatTopicThreshold) {
          tuple._1
        }
    }.toList collect { case s: String => s }
  }


  private def doKmeans(sparse0: RDDLabeledPoint, iterations:Int=20, numClusters: Int = 10, proportion: Double = 0.9, initSteps:Int = 5): Option[(KMeansModel, Map[Int,String])] = {
    try {
      // Load and parse the data

      val sparse1:RDDLabeledPoint=getOrThrow(RDDLabeledPoint.resetColumnIndices(sparse0))

      val numIterations = iterations


      val dfIn=sparse1.transformRDDToDataFrame()
      val kmeans = new org.apache.spark.ml.clustering.KMeans()
        .setK(numClusters)
        .setFeaturesCol("features")
        .setMaxIter(iterations)
        .setInitMode("k-means||")
        .setInitSteps(initSteps)
        .setSeed(1L)
      val model = kmeans.fit(dfIn)


      val WSSSE = model.computeCost(dfIn)


      addLine("Within Set Sum of Squared Errors = " + WSSSE)


      Some((model,sparse1.columnIndexNameMap()))
    }
    catch {
      case e: Exception =>
        logger.warn(e.getMessage, e)
        Try(RDDLabeledPoint.printSparseLocally(sparse0,"kmeansError", pconfig.get(PipelineConfiguration.pipelineOutputRoot)))
        None
    }
  }


}
