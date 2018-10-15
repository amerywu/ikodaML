package ikoda.ml.cassandra

import com.datastax.driver.core.ResultSet
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.{CellTuple, ColumnHeadTuple, RDDLabeledPoint}
import ikoda.utilobjects.DebugObjectTracker
import ikoda.utils.TicToc
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object BatchToCassandraFlushStaging extends BatchToCassandraTrait with Logging {


  val q: mutable.Queue[(String, Int)] = new mutable.Queue[(String, Int)]()
  val flushRecord: scala.collection.concurrent.TrieMap[String, Long] = new scala.collection.concurrent.TrieMap[String, Long]()


  def runFlushMonitor: Unit = {
    val distinctq = q.distinct
    q.clear()
    val tt: TicToc = new TicToc()
    logger.info(tt.tic("\n\n--------------------\nBatchToCassandraFlushStaging\n--------------------\n", 120000))
    while (distinctq.nonEmpty) {


      logger.debug("Calls in flush queue: " + distinctq.length)
      val tuple: Tuple2[String, Int] = distinctq.dequeue()
      doFlush(tuple._1, tuple._2)
      logger.info("\n----------- Flush Completed for " + tuple._1 + " -----------\n")
      stagingOrphanCheck


      Thread.sleep(500)
    }
    logger.info(tt.toc("\n\n--------------------\nBatchToCassandraFlushStaging\n--------------------\n"))
  }


  def stagingOrphanCheck(): Unit = {
    try {
      flushRecord.foreach {
        r =>
          if ((System.currentTimeMillis() - r._2) > 43200000) {
            processFlush(1, 0, r._1)
            flushRecord.put(r._1, System.currentTimeMillis())
          }
      }
    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }


  def processFlush(count: Long, flushThreshold: Int, keyspaceName: String): Unit = {
    try {
      count match {
        case x if (x > flushThreshold) =>
          synchronized {
            logger.info(s"\n-----------\nProcessFlush: Loading $x rows from staging " + keyspaceName + "\n-----------\n\n1. Load sparse from staging \n2. Load Supplement from staging (if required)\n3. Save supplement\n4. Save sparse")
            val sparse0: RDDLabeledPoint = loadSparseFromStaging(keyspaceName)

            sparse0.rowCountEstimate match {

              case x if (x > 0) =>
                sparse0.repartition(1000)
                logger.info("Loaded: " + sparse0.info)
                flushRecord.put(keyspaceName, System.currentTimeMillis())
                transferSparseDataFromStagingToFinal(keyspaceName, sparse0)
                resetStaging(keyspaceName)
              case _ => logger.warn("Empty dataset....aborting")
            }
          }
        case _ => logger.trace("Not enough records. Will not flush")
      }
    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  private def resetStaging(keySpaceStaging: String): Unit = {
    truncateKeyspace(keySpaceStaging)
    resetColumnsInStaging(keySpaceStaging)
  }

  private def resetColumnsInStaging(keySpaceStaging: String): Unit = {
    try {
      val fpconfig: PipelineConfiguration = new PipelineConfiguration
      fpconfig.config(PipelineConfiguration.keyspaceName, "f" + keySpaceStaging)
      val sdLoader: SparseDataToRDDLabeledPoint = new SparseDataToRDDLabeledPoint(fpconfig)
      val columnMap: mutable.ListMap[Int, ColumnHeadTuple] = sdLoader.loadColumns()

      val pconfig: PipelineConfiguration = new PipelineConfiguration
      pconfig.config(PipelineConfiguration.keyspaceName, keySpaceStaging)
      val sdi: SparseDataInput = new SparseDataInput(pconfig)
      sdi.updateColumnsFromScratch(columnMap)
    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  def doFlush(keyspaceName: String, flushThreshold: Int): Unit = {
    try {
      if (CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspaceName).flush) {
        registerKeyspaceForSparse(keyspaceName)
        registerKeyspaceForSparse("f" + keyspaceName)
        val resultSett = countQuery(keyspaceName)

        resultSett.isSuccess match {
          case true =>
            val countt: Try[Long] = Try(resultSett.get.one().getLong(0))
            val count: Long = countt.getOrElse(0L)
            logger.info(s"There are ${count} records in $keyspaceName. Threshold for flushing is $flushThreshold.")
            processFlush(count, flushThreshold, keyspaceName)


          case false =>
            logger.warn(resultSett.failed.get.getMessage, resultSett.failed.get)
            if (resultSett.failed.get.getMessage.contains("Cassandra failure during read query")) {
              logger.warn("\n\n\nTruncating staging. Data permanently lost\n\n\n")
              resetStaging(keyspaceName)
            }
            if (resultSett.failed.get.getMessage.toLowerCase().contains("timeout")) {
              logger.warn("\n\n\nTimeout when counting\n\n\n")
              processFlush(100, flushThreshold, keyspaceName)


            }
        }
      }
      else {
        logger.info(s"doFlush: $keyspaceName configured not to flush")
      }
      ///////////////////888888888888888888888888
      DebugObjectTracker.remove("mappedUuid1")
      DebugObjectTracker.remove("mappedUuid2")
      DebugObjectTracker.remove("mappedUuid3")
      ///////////////////888888888888888888888888
    }
    catch {
      case e: Exception => logger.warn(
        "\n\nERROR: Could not flush staging sparse data in" + keyspaceName + "\n" + e.getMessage, e)
        truncateKeyspace(keyspaceName)
    }
  }


  def loadSparseFromStaging(keyspaceName: String): RDDLabeledPoint = {
    try {
      val sparse: RDDLabeledPoint = loadSparseDataFromCassandra(keyspaceName)
      logger.info(s"\n----------\nLoaded staging data to sparse from $keyspaceName\n----------\n")

      CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspaceName).trim match {
        case true => removeLowFreqFromSparse1(sparse)
        case false => sparse
      }

      ///////////////8888888888888888888888

      /** *******
        *logger.debug("\n***************loadSparseFromStaging*******************")
        *logger.debug(sparseSmaller.show(5))
        * *
        * val sparseSchema:RDDLabeledPoint=loadSparseSchemaFromCassandra("f"+keyspaceName)
        * *
        *logger.debug("loadSparseFromStaging got schema")
        * val sparseWithFinalSchemao:Option[RDDLabeledPoint]=sparseSchema.transformToRDDLabeledPointWithSchemaMatchingThis(sparseSmaller)
        * *
        *sparseWithFinalSchemao.isDefined match
        * {
        * case true =>
        *logger.info("Staging data with final schema: "+sparseWithFinalSchemao.get.info())
        * *
        *
        *sparseWithFinalSchemao.get
        * case false => throw new IKodaMLException(s"FAILED to convert $keyspaceName to final schema f$keyspaceName")
        * } ***/
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }

  def transferSparseDataFromStagingToFinal(keyspaceName: String, sparse0: RDDLabeledPoint): Unit = {
    val pconfig: PipelineConfiguration = new PipelineConfiguration
    pconfig.config(PipelineConfiguration.keyspaceName, "f" + keyspaceName)
    pconfig.config(PipelineConfiguration.keyspaceUUID, SparseDataInput.getUuid("f" + keyspaceName))

    val sdi: SparseDataInput = new SparseDataInput(pconfig)

    if (flushSupplementaryData(sparse0, keyspaceName)) {
      logger.debug("transferSparseDataFromStagingToFinal " + sparse0.info())
      val uidColIdx = sparse0.getColumnIndex(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspaceName).uidinsparse)
      val q: mutable.Queue[Int] = new mutable.Queue[Int]()
      q += uidColIdx
      logger.info("Removing " + q.size + " columns")
      val sparseOuto = RDDLabeledPoint.removeColumnsDistributed(sparse0, q.toSet)
      if (sparseOuto.isDefined) {
        val sparseOut = sparseOuto.get
        ///////////////8888888888888888888888
        logger.debug("\n----------transferSparseDataFromStagingToFinal-------------\n")
        val uidseq = sparseOut.sparseData().map(r => r._3)
        logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid1", uidseq.collect()))
        logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid2", uidseq.collect()))
        logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid3", uidseq.collect()))
        ///////////////8888888888888888888888
        sdi.insertAllData(sparseOut)
        logger.info("\n---------------\nAll data inserted. Truncating staging\n---------------\n")
      }

    }
  }


  private def runSupplementFlush(keyspaceName: String): Boolean = {
    sparseSupplementSet.contains(keyspaceName) match {
      case false =>
        logger.info(s"\n----------\nSupplement does not exist in $keyspaceName\n----------\n")
        val pconfig: PipelineConfiguration = new PipelineConfiguration
        pconfig.config(PipelineConfiguration.keyspaceName, keyspaceName)
        var ssmm: SparseSupplementModelMaker = new SparseSupplementModelMaker(pconfig)
        ssmm.supplementExists()
      case true =>
        sparseSupplementSet += keyspaceName
        true
    }
  }


  private def loadSparseSupplementFromStaging(keyspaceName: String): DataFrame = {
    try {
      logger.info("loadSparseSupplementFromStaging")
      val pconfig: PipelineConfiguration = new PipelineConfiguration
      pconfig.config(PipelineConfiguration.keyspaceName, keyspaceName)
      pconfig.config(PipelineConfiguration.keyspaceUUID, SparseDataInput.getUuid("f" + keyspaceName))
      val ssr: SparseSupplementRetriever = new SparseSupplementRetriever(pconfig)
      val ssdf: DataFrame = ssr.loadSparseSupplement()
      logger.debug(s"loaded ${ssdf.count()} rows from sparseSupplement in staging")
      val sqlContext = getSparkSession().sqlContext
      import sqlContext.implicits._
      val ssdff = ssdf.filter($"uuid".isNotNull)

      ///////////////8888888888888888888888
      logger.debug("\n----------loadSparseSupplementFromStaging-------------\n")
      val seqUuid: Seq[String] = ssdff.rdd.map(r => r.getAs[String]("uuid")).collect()
      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid1", seqUuid))
      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid2", seqUuid))
      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid3", seqUuid))
      ///////////////8888888888888888888888

      logger.debug(s"Filtered for uuid not null. Remaining row count: ${ssdff.count()}")
      ssdff
    }
    catch {
      case e: Exception => throw new IKodaMLException("loadSparseSupplementFromStaging: " + e.getLocalizedMessage, e)
    }
  }


  private def transferSparseSupplementToFinal(keyspaceName: String, df: DataFrame): Unit = {
    try {
      val pconfig: PipelineConfiguration = new PipelineConfiguration
      pconfig.config(PipelineConfiguration.keyspaceUUID, SparseDataInput.getUuid("f" + keyspaceName))
      pconfig.config(PipelineConfiguration.keyspaceName, "f" + keyspaceName)
      val ssmm: SparseSupplementModelMaker = new SparseSupplementModelMaker(pconfig)
      ssmm.createTablesIfNotExist(df)

      val ssdi: SparseSupplementDataInput = new SparseSupplementDataInput(pconfig)

      ///////////////8888888888888888888888
      logger.debug("\n----------transferSparseSupplementToFinal-------------\n")
      val seqUuid: Seq[String] = df.rdd.map(r => r.getAs[String]("uuid")).collect()
      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid1", seqUuid))
      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid2", seqUuid))
      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid3", seqUuid))
      ///////////////8888888888888888888888


      ssdi.insertSupplementaryData(df)
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getLocalizedMessage, e)
    }
  }

  private def flushSupplementaryData(sparse0: RDDLabeledPoint, keyspaceName: String): Boolean = {
    try {
      if (CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspaceName).supplement) {
        logger.info(s"\n----------\nFlushing Supplementary data in $keyspaceName\n----------\n")

        registerKeyspaceForSparse(keyspaceName)
        registerKeyspaceForSparse("f" + keyspaceName)

        if (runSupplementFlush(keyspaceName)) {
          logger.info(s"\n----------\nSparse supplement exists in $keyspaceName. Flushing data.\n----------\n")
          logger.info("calling loadSparseSupplementFromStaging")
          transferSparseSupplementToFinal(keyspaceName, loadSparseSupplementFromStaging(keyspaceName))
          true
        }
        else {
          logger.warn(
            "Supplement table cannot be created until supplement data has arrived. Waiting for supplement data before " +
              "flushing.")
          false
        }
      }
      else {
        true
      }
    }
    catch {
      case e: Exception => logger.warn(
        "\n\nERROR: Could not flush staging supplementary data " + keyspaceName + "\n" + e.getMessage, e)
        false
    }
  }

  @throws(classOf[IKodaMLException])
  private def removeLowFreqFromSparse1(sparse0: RDDLabeledPoint, medianAdjustment: Int = 0): RDDLabeledPoint = {
    try {
      sparse0.rowCountEstimate match {
        case x if (x > 0) =>
          val tt: TicToc = new TicToc
          logger.info(tt.tic("Removing low freq columns from sparse"))
          val colSums: Map[Int, CellTuple] = RDDLabeledPoint.colSums(sparse0)

          val valueList: List[Double] = colSums.map(ct => ct._2.value).toList.sorted
          val median = valueList(valueList.length / 2)
          val toDropFromSparse = colSums.values.filter(ct => ct.value < (median + medianAdjustment)).map(
            ct => ct.colIndex
          ).toSeq


          logger.info(s"Median is $median")
          logger.info(s"Adjusted criteria is ${median + medianAdjustment}")
          logger.info(s"Current column count ${sparse0.columnCount}")
          logger.info(s"Removing ${toDropFromSparse.length} columns ")
          val sparseOuto = RDDLabeledPoint.removeColumnsDistributed(sparse0, toDropFromSparse.toSet)
          if (sparseOuto.isDefined) {
            val sparseOut = sparseOuto.get
            logger.info(s"---New column count ${sparseOut.columnCount}---")
            logger.info(tt.toc)
            sparseOut
          }
          else {
            sparse0
          }
        case _ => sparse0
      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage(), e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  @throws(classOf[IKodaMLException])
  def loadSparseDataFromCassandra(keyspaceName: String): RDDLabeledPoint = {
    try {
      val pconfig: PipelineConfiguration = new PipelineConfiguration
      pconfig.config(PipelineConfiguration.keyspaceName, keyspaceName)
      val loader = new SparseDataToRDDLabeledPoint(pconfig)
      loader.loadFromCassandra
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def loadSparseSchemaFromCassandra(keyspaceName: String): RDDLabeledPoint = {
    try {
      val pconfigforschema: PipelineConfiguration = new PipelineConfiguration
      pconfigforschema.config(PipelineConfiguration.keyspaceName, keyspaceName)
      val schemaLoader = new SparseDataToRDDLabeledPoint(pconfigforschema)
      schemaLoader.loadSchemaFromCassandra
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }

}
