package ikoda.ml.pipeline

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.caseclasses.{KmeansClusterMember, LemmatizedSentence, SentenceSupplementData}
import ikoda.ml.cassandra.{QueryExecutor, SparseDataToRDDLabeledPoint, SparseSupplementRetriever}
import ikoda.sparse.{ColumnHeadTuple, RDDLabeledPoint}
import ikoda.utilobjects.{SimpleLog, SparkConfProviderWithStreaming}
import ikoda.utils.{CSVSpreadsheetCreator, IDGenerator, Spreadsheet}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try


class PhraseAnalysis(pconfig: PipelineConfiguration,sparseDatao: Option[RDDLabeledPoint]) extends Logging with SimpleLog with SparkConfProviderWithStreaming with QueryExecutor {


  //var sparseDatao: Option[RDDLabeledPoint] = None
  lazy val loader = loadLoader()
  lazy val columnMap: mutable.ListMap[Int, ColumnHeadTuple] = loadColumnMap()

  val lpCache:mutable.HashMap[String,LabeledPoint]=new mutable.HashMap[String,LabeledPoint]()


  def setSimpleLog(path: String, fileName: String): Unit = {
    initSimpleLog(path, fileName)
  }

  private def loadColumnMap(): mutable.ListMap[Int, ColumnHeadTuple] =
  {
    val sd2Rdd:SparseDataToRDDLabeledPoint=new SparseDataToRDDLabeledPoint(pconfig)
    sd2Rdd.loadColumns()
  }

  @throws(classOf[IKodaMLException])
  private def openInputCsv(spreadsheetName: String): CSVSpreadsheetCreator = {
    try {
      Spreadsheet.getInstance().initCsvSpreadsheet(spreadsheetName, logger.name, pconfig.get(PipelineConfiguration.phraseAnalysisDataSourceRootPath))
      Spreadsheet.getInstance().getCsvSpreadSheet(spreadsheetName).loadCsv(spreadsheetName, pconfig.get(PipelineConfiguration.phraseAnalysisInputCsvUidColumnName))
      Spreadsheet.getInstance().getCsvSpreadSheet(spreadsheetName)
    }
    catch {

      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  private def loadLoader(): SparseDataToRDDLabeledPoint = {
    try {


      val loader: SparseDataToRDDLabeledPoint = new SparseDataToRDDLabeledPoint(pconfig)
      loader


    }
    catch {

      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def getTermsInTarget(termsSeqo: Option[Seq[KmeansClusterMember]], target: String): Option[Seq[KmeansClusterMember]] = {
    try {
      termsSeqo.isDefined match {
        case false => None
        case true =>
          if (termsSeqo.get.isEmpty) {
            None
          }
          else {
            val termsSeq = termsSeqo.get
            val termsInTargetSeq = termsSeq.filter(kcm => kcm.target == target)
            val maxIter: Int = termsInTargetSeq
              .maxBy(_.iteration).iteration


            maxIter > 8 match {
              case true => Option(termsInTargetSeq.filter(kcm => kcm.iteration == maxIter || kcm.iteration == (maxIter / 2).toInt))
              case false => Option(termsInTargetSeq.filter(kcm => kcm.iteration == maxIter))
            }
          }
      }
    }
    catch {

      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }



  private def initializeClusterAnalysis(csvName: String): TermsDataReductionByClustering = {

    val pconfig2 = new PipelineConfiguration
    pconfig2.config(PipelineConfiguration.lda_topicCount, "2")
    pconfig2.config(PipelineConfiguration.km_clusterCount, "2")
    pconfig2.config(PipelineConfiguration.lda_topicCountByTarget, "2")
    pconfig2.config(PipelineConfiguration.km_clusterCountByTarget, "2")
    pconfig2.config(PipelineConfiguration.pipelineOutputRoot, pconfig.get(PipelineConfiguration.pipelineOutputRoot))
    pconfig2.config(PipelineConfiguration.km_countTopClusterValues, "10")
    pconfig2.config(PipelineConfiguration.lda_minTopicWeightByTarget, "0.001")
    pconfig2.config(PipelineConfiguration.lda_minTopicWeight, "0.002")
    pconfig2.config(PipelineConfiguration.km_termRepeatAcrossClusters, "2")
    pconfig2.config(PipelineConfiguration.lda_termRepeatAcrossTopics, "2")
    pconfig2.config(PipelineConfiguration.phraseAnalysisOverwriteClusterCsv, "false")

    val tdrc = new TermsDataReductionByClustering(pconfig2)
    tdrc.setSimpleLog(pconfig.get(PipelineConfiguration.pipelineOutputRoot), "pa.log")
    tdrc
  }


  private def screenForProcessSecondTierLDA(sentences: Seq[LemmatizedSentence],subset:String,csvName:String ):Unit=
  {
    sentences.size > 50 match
    {
      case true => processSecondTierLDAAnalysis(sentences,subset,csvName)
      case false => cacheSentences(csvName,sentences,false)
    }
  }

  @throws(classOf[IKodaMLException])
  private def processSecondTierLDAAnalysis(sentences: Seq[LemmatizedSentence],subset:String,csvName:String): Unit = {
    try {

      addLine("Second Tier LDA")
      addLine("Running LDA on sentences retrieved after initial k means analysis on "+subset)
      addLine(s"There are ${sentences.size} sentences")
      sparseDatao.isDefined match {
        case true =>
          val sparseData = sparseDatao.get
          val dataArray: Array[Tuple3[LabeledPoint, Int, String]] = sentences.map {
            e => e.sparseRow
          }.toArray

          val rddData: RDD[Tuple3[LabeledPoint, Int, String]] = spark.sparkContext.parallelize(dataArray)
          val sparse1: RDDLabeledPoint = new RDDLabeledPoint(rddData, columnMap.map(c=> c._1 ->   c._2), sparseData.copyTargetMap, pconfig.get(PipelineConfiguration.phraseAnalysisSecondTierFileName))

          val tdrc: TermsDataReductionByClustering = initializeClusterAnalysis(csvName)
          tdrc.kmeansProcessTermSelectionNoRecursion(sparse1,subset)

        case false => throw new IKodaMLException("secondTier: Failed to retrieve data ")
      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def analyzeClusters(csvName: String, termsInTargeto: Option[Seq[KmeansClusterMember]], runNextTier: Boolean = true): Unit = {
    try {
      if (termsInTargeto.isDefined) {
        val termsInTarget: Seq[KmeansClusterMember] = termsInTargeto.get
        val clusterIds: Set[String] = termsInTarget.map {
          kcm => kcm.cluster
        }.distinct.toSet

        val totalClusterCount=clusterIds.size
        var currentCluster=0



        clusterIds.foreach {
          cid =>
            addLine(s"\n*--------------------------------------\n$csvName \nCluster Id: $cid:  \n$currentCluster out of  $totalClusterCount clusters\n")

            currentCluster = currentCluster +1
            val topTermsInClustero: Option[Seq[KmeansClusterMember]] = getTopTermsInCluster(cid, termsInTarget)

            if (topTermsInClustero.isDefined) {
              addLine(s"Top terms in cluster are ${topTermsInClustero.get.mkString("\n")}")


              val subset=topTermsInClustero.get(0).target
              addLine("Subset: " + subset)

              val sentencesInCluster: Seq[LemmatizedSentence] = getLemmasforTerms(csvName, topTermsInClustero.get.map(e => (e, Seq.empty[LemmatizedSentence])))
              cacheSentences(csvName, sentencesInCluster)
              endAnalysis(csvName) match
              {
                case true =>
                case false =>screenForProcessSecondTierLDA(sentencesInCluster,subset,csvName)
              }


            }
        }
      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  def endAnalysis(csvName:String):Boolean =
  {
    csvName.toLowerCase().contains("second")
  }

  def retrieveSupplement(lsSeq:Seq[LemmatizedSentence]):  Option[Seq[LemmatizedSentence]] =
  {
    val ssr:SparseSupplementRetriever=new SparseSupplementRetriever(pconfig)
    ssr.getLemmatizedSentences(lsSeq)
  }

  def cacheSentences(csvName: String, sentencesIn: Seq[LemmatizedSentence],ranSecondTier:Boolean=true): Unit = {




    val sentenceso: Option[Seq[LemmatizedSentence]] = retrieveSupplement(sentencesIn)
    sentenceso.isDefined match {

      case true =>

        val sentences=sentenceso.get
        logger.info(s"cacheSentences Incoming sentence count was ${sentencesIn.size}. Found ${sentences.size} supplementary records")


        val lemmatizedPhrase = "lemmatizedPhrase"
        val rawPhrase = "rawPhrase"
        val targetName = "targetName"
        val targetId = "targetId"
        val cluster = "clusterId"
        val term1 = "term1"
        val term2 = "term2"
        val valueTerm1 = "valueTerm1"
        val valueTerm2 = "valueTerm2"
        val subset = "subset"
        val ranSecondTierH = "ranSecondTier"
        val sentenceUuid = "sentenceUuid"

        addLine(s"cacheSentences Caching ${sentences.size} sentences derived from $csvName")

        sentences.foreach {
          s =>
            val id = String.valueOf(IDGenerator.getInstance().nextIDInMem())
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, lemmatizedPhrase, s.lemmatizedSentence)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, rawPhrase, s.rawSentence)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, targetName, s.targetName)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, targetId, s.target.toString)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, cluster, s.clusterId.toString)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, term1, s.term1)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, valueTerm1, s.term1Value.toString)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, term2, s.term2)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, valueTerm2, s.term2Value.toString)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, subset, s.subset)
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, ranSecondTierH, String.valueOf(ranSecondTier))
            Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").addCell(id, sentenceUuid, s.sparseRow._3)
        }
        if (sentences.size > 0) {
          Spreadsheet.getInstance().getCsvSpreadSheet(s"${csvName}_lemmatizedOutput").printCsvAppend(s"${csvName.replaceAll(".csv", "")}_lemmatizedOutput.csv")
        }

      case false => logger.warn(s"\n\nIncoming sentence count was ${sentencesIn.size}. No supplementary records retrieved \n\n")
    }


  }


  def generateQuery(kcmSeq: Seq[KmeansClusterMember], labelo: Option[Double]): String = {
    try {
      val sb = new StringBuilder
      sb.append("select *  from " + pconfig.get(PipelineConfiguration.keyspaceName) + ".\"sparseData\" where ")
      val itr = kcmSeq.iterator
      while (itr.hasNext) {
        sb.append(s" indices contains ${itr.next().termNumeric}")
        if (itr.hasNext) {
          sb.append(" and ")
        }
      }
      if (labelo.isDefined) {
        sb.append(s" and label = ${labelo.get}")
      }
      sb.append(" allow filtering;")
      logger.debug(sb.toString())
      sb.toString()
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  val getSentencesForTermCombinations = (csvName: String) => (headTuple: Tuple2[KmeansClusterMember, Seq[LemmatizedSentence]], tailTuple: Tuple2[KmeansClusterMember, Seq[LemmatizedSentence]]) => {

    sparseDatao.isDefined match {
      case true =>
        val sparse0 = sparseDatao.get
        val targeto = sparse0.getTargetId(headTuple._1.target)
        val colIds = Set(sparse0.getColumnIndex(headTuple._1.term), sparse0.getColumnIndex(tailTuple._1.term))

        logger.debug(s"Getting lemmas for ${headTuple._1.target}\n${headTuple._1} \nand \n${tailTuple._1}")
        if (headTuple._1.target.trim == pconfig.get(PipelineConfiguration.phraseAnalysisAllTargetsColValue)) {

          val collected: Seq[LemmatizedSentence] = registerLemmatizedSentences(csvName, headTuple._1, tailTuple._1, RDDLabeledPoint.getRowsContainingColIdxAndMatchesLabelUnchangedSchema(sparse0,colIds, None))

          //logger.debug(collected.mkString("\n"))
          (headTuple._1, headTuple._2 ++ collected)


          //registerLemmatizedSentences(csvName, headkcm, tailkcm, loader.loadFromCassandra(generateQuery(Seq(headkcm,tailkcm),None)))
        }
        else if (targeto.isDefined) {
          val collected: Seq[LemmatizedSentence] = registerLemmatizedSentences(csvName, headTuple._1, tailTuple._1, loader.loadFromCassandra(generateQuery(Seq(headTuple._1, tailTuple._1), targeto)))
          //logger.debug(collected.mkString("\n"))
          (headTuple._1, headTuple._2 ++ collected)

        }
        else {
          logger.warn(s"No target found for ${headTuple._1.target}")
          (headTuple._1, headTuple._2)

        }

      case false => throw new IKodaMLException("Could not retrieve schema.")
    }
  }


  @throws(classOf[IKodaMLException])
  def getLemmasforTerms(csvName: String, topTermsInCluster: Seq[Tuple2[KmeansClusterMember, Seq[LemmatizedSentence]]]): Seq[LemmatizedSentence] = {
    try {
      val lemmatize = getSentencesForTermCombinations(csvName)

      def foldLeftRoutine(topTermsInClusterMutableIn: ListBuffer[Tuple2[KmeansClusterMember, Seq[LemmatizedSentence]]]): Seq[LemmatizedSentence] = {
        val sentences: ListBuffer[LemmatizedSentence] = new ListBuffer[LemmatizedSentence]
        while (topTermsInClusterMutableIn.size >= 2) {

          topTermsInClusterMutableIn.sortBy(-_._1.value)
          val topTermsInClusterWithAccumulatedSentences: Tuple2[KmeansClusterMember, Seq[LemmatizedSentence]] = topTermsInClusterMutableIn.tail.foldLeft(topTermsInClusterMutableIn.head)(lemmatize(_, _))

          sentences ++= topTermsInClusterWithAccumulatedSentences._2
          logger.debug(s"Accrued sentence count is ${sentences.size} after ${topTermsInClusterMutableIn.head} with after ${topTermsInClusterMutableIn.tail}")
          topTermsInClusterMutableIn.remove(0)
        }
        sentences
      }

      if (sparseDatao.isDefined) {
        val topTermsInClusterMutable: ListBuffer[Tuple2[KmeansClusterMember, Seq[LemmatizedSentence]]] = ListBuffer.empty ++= topTermsInCluster
        foldLeftRoutine(topTermsInClusterMutable)


      }
      else {
        throw new IKodaMLException("No source data acquired")
      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  def combineLemmas(lp: LabeledPoint, sparse0:RDDLabeledPoint): String =
  {
    try {
      val sb: StringBuffer = new StringBuffer()
      lp.features.toSparse.indices.foreach {
        idx =>
          sb.append(sparse0.getColumnNameAsOption(idx).getOrElse("?"))
          sb.append(" ")
      }
      sb.toString
    }
    catch
      {
        case e:Exception=> "not in supplement"
      }
  }




  @throws(classOf[IKodaMLException])
  def registerLemmatizedSentences(csvName: String, kcm1: KmeansClusterMember, kcm2: KmeansClusterMember, sparseo: Option[RDDLabeledPoint]): Seq[LemmatizedSentence] = {
    try {
      if (sparseo.isDefined) {


        val sparse0 = sparseo.get
        //logger.debug(s"Retrieved ${sparse0.getRowCountCollected} sentences with cluster id ${kcm2.cluster}")
        val seqSentences=sparse0.sparseData().collect.map {
          r =>
            lpCache.put(r._3,r._1)
                LemmatizedSentence(r, "", "", kcm2.cluster, r._1.label, "", kcm1.term, kcm1.value, kcm2.term, kcm2.value,kcm1.target)

        }.toSeq

        seqSentences
      }
      else {
        logger.warn(s"\n\nNo data found for term combination ${kcm1.term} + ${kcm2.term}\n\n")
        Seq.empty[LemmatizedSentence]
      }
    }
    catch {

      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def getTopTermsInCluster(clusterId: String, termsInTarget: Seq[KmeansClusterMember]): Option[Seq[KmeansClusterMember]] = {
    try {
      val top = pconfig.getAsInt(PipelineConfiguration.phraseAnalysisTopTermCountPerClusterForAnalysis)
      termsInTarget.size match {

        case x if x >= top =>
          Some(termsInTarget.filter(kcm => kcm.cluster == clusterId).sortBy(-_.value).take(top))
        case x if x == 0 => None
        case x if x > 0 && x < top =>
          Some(termsInTarget.filter(kcm => kcm.cluster == clusterId).sortBy(-_.value).take(termsInTarget.size))
      }
    }
    catch {

      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def phraseAnalysisFromReduction(csvName: String): Unit = {
    try {

      addLine("\n\n*******************phraseAnalysisFromReduction "+csvName+"****************\n")
      phraseAnalysis(csvName)
      sparseDatao.get.info()

    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  private def phraseAnalysis(csvName: String): Unit = {
    try {

      addLine("\n\nPhrase Analysis for data from " + csvName)

      Spreadsheet.getInstance().initCsvSpreadsheet(s"${csvName}_lemmatizedOutput", "ikoda.ml", pconfig.get(PipelineConfiguration.phraseAnalysisReportPath))


      val termsSeq: Seq[KmeansClusterMember] = groupedTermsSeq(openInputCsv(csvName))
      val targetSet: Set[String] = termsSeq.map {
        kcm => kcm.target
      }.distinct.toSet


      var count=1
      targetSet.foreach {
        target =>

          addLine("Target: " + target)
          addLine(s"Progress through targets: $target is $count out of ${targetSet.size}")
          val termsInTargeto: Option[Seq[KmeansClusterMember]] = getTermsInTarget(Option(termsSeq), target)
          count = count+1

          analyzeClusters(csvName, termsInTargeto)
      }


    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def phraseAnalysisSecondTier(csvName: String): Unit = {
    try {

      logger.info("\n\nphraseAnalysisSecondTier " + csvName)
      phraseAnalysis(csvName)
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def groupedTermsSeq(csv: CSVSpreadsheetCreator): Seq[KmeansClusterMember] = {
    try {
      val rowId: String = "A_RowId"
      val analysisType: String = "AnalysisType"
      val cluster: String = "Cluster/Topic"
      val iteration: String = "Iteration"
      val target: String = "Target"
      val term: String = "Term"
      val value: String = "Value"
      sparseDatao.isDefined match {

        case true =>
          csv.getData.asScala.map {
            case (id, cols) =>

              val id: Int = cols.getOrDefault(rowId, "-1").toInt
              val atype: String = cols.getOrDefault(analysisType, "NA")
              val kcluster: String = cols.getOrDefault(cluster, "-1")
              val itrn: Int = cols.getOrDefault(iteration, "-1").toInt
              val targetName: String = cols.getOrDefault(target, "NA")
              val termWord: String = cols.getOrDefault(term, "NA")
              val clusterValue: Double = cols.getOrDefault(value, "-1.0").toDouble


              new KmeansClusterMember(id, atype, kcluster, itrn, targetName, termWord, clusterValue, sparseDatao.get.getTargetId(targetName), sparseDatao.get.getColumnIndex(termWord))
          }.toSeq.filter(kmc => kmc.termNumeric >1)
        case false => throw new IKodaMLException("Could not retrieve schema")
      }
    }
    catch {

      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


}
