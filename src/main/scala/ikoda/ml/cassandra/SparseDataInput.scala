package ikoda.ml.cassandra

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.{ColumnHeadTuple, RDDLabeledPoint}
import ikoda.utilobjects.{DebugObjectTracker, SparkConfProviderWithStreaming, UtilFunctions}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try


object SparseDataInput extends Logging with QueryExecutor with UtilFunctions {
  @throws(classOf[IKodaMLException])
  private[cassandra] def getUuid(keyspaceName: String): String = {
    try {
      val query = "SELECT * FROM " + keyspaceName + ".keyspaceuuid"
      val resultSett: Try[ResultSet] = executeQuery(query)
      if (resultSett.isSuccess) {
        val t = Try(resultSett.get.one().getString(0))
        t.getOrElse("failed")

      }
      else {
        throw new IKodaMLException("No uuid found in " + keyspaceName)
      }
    }
    catch {
      case e: Exception => throw new IKodaMLException(keyspaceName + "  " + e.getMessage, e)
    }
  }

  var counter = 0
}
/**
  * Persists sparse data to Cassandra along with complementary human readable column name map and target map
  */
class SparseDataInput(pconfig: PipelineConfiguration) extends Logging with QueryExecutor with SparseBatchDelete {
  lazy val batchuuid = UUIDs.timeBased().toString

  val sqlContext: SQLContext = getSparkSession().sqlContext
  val keyspace: String = pconfig.get(PipelineConfiguration.keyspaceName)
  var currentRow = 0;
  var sentenceWordCache: ArrayBuffer[(String, Int, String, String)] = new ArrayBuffer
  var oldNewIdCache: ArrayBuffer[Tuple3[Double, String, Long]] = new ArrayBuffer


  @throws(classOf[IKodaMLException])
  def validateUuid(): Unit = {
    try {
      val query = "SELECT * FROM " + pconfig.get(PipelineConfiguration.keyspaceName) + ".keyspaceuuid"
      logger.debug(query)
      val resultSett: Try[ResultSet] = executeQuery(query)
      if (resultSett.isSuccess) {
        val dbuuidt: Try[String] = Try(resultSett.get.one().getString("uuid"))
        if (dbuuidt.isSuccess) {
          logger.info("\ndbuuid: " + dbuuidt.get + "\ninuuid: " + pconfig.get(PipelineConfiguration.keyspaceUUID))
          require(dbuuidt.get.length > 10)
          require(pconfig.get(PipelineConfiguration.keyspaceUUID).length > 10)
          require(dbuuidt.get == pconfig.get(PipelineConfiguration.keyspaceUUID))
        }
      }
    }
    catch {
      case e: Exception => throw new IKodaMLException("\n\n" + e.getMessage, e)
    }
  }


  private def  performChecksum(lpArrayReAligned: Array[(LabeledPoint, Int, String)], sparse: RDDLabeledPoint, proportionAllowed: Double): Boolean = {
    try {
      logger.info("\n--------------\nchecksum\n--------------")


      val take: Int = lpArrayReAligned.length > 500 match {
        case true => lpArrayReAligned.length / 100
        case false => lpArrayReAligned.length
      }
      val subset = lpArrayReAligned.take(take)
      val query = "select count(hashcode) from " + pconfig.get(PipelineConfiguration.keyspaceName) + ".\"sparseData\" where hashcode = "
      val hashcodes: Seq[Int] = subset.toSeq.map {

        r =>
          r._2
      }
      logger.info("Subset: " + hashcodes.size)


      var matchCount: Long = 0
      hashcodes.foreach {
        hc =>
          val rst = executeQuery(query + hc)
          rst.isSuccess match {
            case true =>
              val resultt = Try(rst.get.one().getLong(0))
              val count: Long = resultt.getOrElse(0L)
              count > 0 match {
                case true =>

                  matchCount = matchCount + 1
                  logger.warn("Duplicated entry on " + hc + " with db entry count of " + resultt + " in " + pconfig.get(PipelineConfiguration.keyspaceName) + " count so far: " + matchCount)
                case _ =>
              }
            case false =>
              throw new IKodaMLException("Checksum Exception " + rst.failed.get.getMessage)
          }
      }


      if (matchCount > 0) {
        val proportion: Double = matchCount.toDouble / hashcodes.size.toDouble
        if (proportion > proportionAllowed) {
          logger.warn("Too many duplicate records in incoming data " + proportion * 100 + "% " + pconfig.get(PipelineConfiguration.keyspaceName))
          deleteBatchFromTables(batchuuid, pconfig.get(PipelineConfiguration.keyspaceName))
          throw new IKodaMLException("Too many duplicates in incoming data")

        }
        else {
          logger.info(
            s"Count: $matchCount Sample ${hashcodes.size} It is estimated that ${proportion * 100} % of records in the incoming data  duplicate extant data. " + pconfig.get(PipelineConfiguration.keyspaceName))
          true
        }
      }
      else {
        logger.info("It is estimated that minimal records from incoming data duplicate extant data " + pconfig.get(PipelineConfiguration.keyspaceName))
        true
      }
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def checksum(lpArrayReAligned: Array[(LabeledPoint, Int, String)], sparse: RDDLabeledPoint): Boolean = {
    try {
      val configt: Try[CassandraKeyspaceConfiguration] = Try(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace))
      configt.isSuccess match {
        case true =>
          val allowedProportion: Double = CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).permittedProportionOfDuplicates
          allowedProportion match {
            case x if (x >= 1) => true
            case _ => performChecksum(lpArrayReAligned, sparse, allowedProportion)

          }
        case false => true
      }
    }
    catch {

      case e: Exception =>
        logger.error(e.getMessage, e)
        throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def insertAllData(sparse: RDDLabeledPoint): Boolean = {
    try {
      validateUuid()
      //logger.debug("insertAllData "+sparse.getColumnIndexNameMap())

      val sparse1o = RDDLabeledPoint.cleanLowerCaseColumns(sparse)
      sparse1o.isDefined match {
        case false => false
        case true =>
          val sparse1=sparse1o.get
          //logger.debug("insertAllData after column cleaning"+sparse1.getColumnIndexNameMap())
          logger.debug("insertAllData: " + sparse1.info())


          logger.info("\n\n- - - - - - \nInserting new sparse data to " + pconfig.get(PipelineConfiguration.keyspaceName) + "\n" + sparse1.info() + "\n- - - - - -\n")
          //logger.debug(sparse1.info())
          val finputVsExtantWordDifferences = inputVsExtantWordDifferences(sparse1.columnHeads())(_)

          val finputVsExtantTargetDifferences = inputVsExtantTargetDifferences(sparse1.getTargets())(_)
          processColumns(finputVsExtantWordDifferences)
          processTargets(finputVsExtantTargetDifferences)
          processSparseData(sparse1)
      }
    }
    catch {

      case e: Exception =>
        logger.error(e.getMessage, e)
        throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def cacheSentenceToWordRecords(sentenceUuid: String, columnIndices: Seq[Int]): Unit = {
    try {
      sentenceWordCache ++= columnIndices.map {
        idx => (UUIDs.timeBased().toString, idx, sentenceUuid, batchuuid)
      }.toSeq

      if (sentenceWordCache.size > 50000) {

        updateSentenceToWordMap()
      }
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }

  @throws(classOf[IKodaMLException])
  private def cacheOldNewId(oldId: Double, newId: String): Unit = {
    try {


      oldNewIdCache += Tuple3(oldId, newId, System.currentTimeMillis())


      if (oldNewIdCache.size > 50000) {

        updateOldNewIdMap()
      }
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def updateSentenceToWordMap(): Unit = {
    try {
      import sqlContext.implicits._
      val dfOut = sentenceWordCache.toDF("uuid", "columnIndex", "sentenceUuid", "batchuuid")
      val dff: DataFrame = dfOut.withColumn("timestamp", when($"uuid".isNull, System.currentTimeMillis()).otherwise(System.currentTimeMillis()))

      dff.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "sentenceToWordMap", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .mode(SaveMode.Append)
        .save()
      logger.info(s"Added ${dfOut.count()} rows to ${pconfig.get(PipelineConfiguration.keyspaceName)}.sentenceToWordMap")

      sentenceWordCache.clear()
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def updateOldNewIdMap(): Unit = {
    try {
      import sqlContext.implicits._
      oldNewIdCache.isEmpty match {
        case true =>
          logger.debug("oldNewIdCache empty ")

        case false =>
          /////////////888888888888888888
          logger.debug("updateOldNewIdMap ")
          logger.debug(oldNewIdCache.take(10).mkString("\n updateOldNewIdMap "))
          DebugObjectTracker.put("mappedUuid1", oldNewIdCache.head._2)
          DebugObjectTracker.put("mappedUuid2", oldNewIdCache(oldNewIdCache.length / 2)._2)
          DebugObjectTracker.put("mappedUuid3", oldNewIdCache(oldNewIdCache.length - 2)._2)
          /////////////888888888888888888
          val dfOut = oldNewIdCache.toDF(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).uidinsparse, "uuid", "dsctimestamp")


          DataSetCache.addToCache(dfOut, keyspace + "oldNewIdMap", 180)
          logger.info(s"Added ${
            dfOut.count()
          } rows to oldnewuid  cache")

          oldNewIdCache.clear()
      }
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  private def alignColumnIndicesForLabeledPoint(lp: LabeledPoint, newColumns: Map[Int, String], extantColumns: Map[String, Int]): Option[(Seq[Int], Array[Double])] = {
    try {
      val aligned: Map[Int, Double] =
        lp.features.toSparse.indices.zipWithIndex.map {
          case (colidx, zidx) =>
            val colNameo = newColumns.get(colidx)
            val colName = colNameo.isDefined match {
              case true => colNameo.get
              case false => throw IKodaMLException(s"Column $colidx is not defined in newColumns")
            }


            val extantIdxo = extantColumns.get(colName)
            if (!extantIdxo.isDefined) {
              throw IKodaMLException(s"Column $colName is not defined in Cassandra columnMap")
            }

            val extantidx = extantIdxo.get

            (extantidx, lp.features.toSparse.values(zidx))

        }.toMap


      val alignedSorted = scala.collection.immutable.TreeMap(aligned.toArray: _*)

      //logger.debug("\n\n\nalignColumnIndicesForLabeledPoint 2\n" + alignedSorted.mkString("\n#"))

      alignedSorted.size == lp.features.toSparse.values.length match {
        case true => Some(new Tuple2(alignedSorted.keySet.toArray, alignedSorted.values.toArray))
        case false =>
          logger.warn("\n\nWARN: Column <-> Value Mismatch\nColumns: " + alignedSorted.size + "\nValues: " + lp.features.toSparse.values.length +
            "\nColumn indices are " + alignedSorted.mkString(",") + "\n Values are " + lp.features.toSparse.values.mkString(" - ") + "\n " + pconfig.get(PipelineConfiguration.keyspaceName))
          logger.warn("Original Indices were of length: " + lp.features.toSparse.indices.length + "\n " + lp.features.toSparse.indices.mkString(" | "))
          None
      }

    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }





  @throws(classOf[IKodaMLException])
  def validateSparseData(): Boolean = {
    try {
      val labeledPointArray: Seq[(LabeledPoint, Int, String)] = getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "sparseData", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .load().collect().map {
        r =>
          (r.getAs[String]("uuid"),
            r.getAs[Int]("hashcode"),
            r.getAs[List[Int]]("indices"),
            r.getAs[List[Double]]("values"),
            r.getAs[Double]("label"),
            r.getAs[String]("uuid")
          )
      }.toSeq.map {
        rowAsSeq =>

          (new LabeledPoint(rowAsSeq._5, org.apache.spark.ml.linalg.Vectors.sparse(rowAsSeq._3.size, rowAsSeq._3.toArray, rowAsSeq._4.toArray)), rowAsSeq._2, rowAsSeq._6)
      }


      val coldIdxMap: mutable.HashMap[Int, Int] = new mutable.HashMap[Int, Int]()
      labeledPointArray.foreach {
        r => r._1.features.toSparse.indices.foreach(idx => coldIdxMap.put(idx, idx))
      }

      val colHeadMap = loadColumns()
      logger.info("\nVALIDATING...\n")
      logger.info(s"Columns in sparse count: " + coldIdxMap.size)
      logger.info(s"Columns in column heads count: " + colHeadMap.size)

      if (colHeadMap.size != coldIdxMap.size) {
        logger.warn("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!\nColumn MISMATCH between actual columns and column heads in " + pconfig.get(PipelineConfiguration.keyspaceName) + ". Deleting batch\n" +
          "!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
        deleteBatchFromTables(batchuuid, pconfig.get(PipelineConfiguration.keyspaceName))
        false
      }
      else {
        true
      }


    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def realignSparseData(lpArray: Array[(LabeledPoint, Int, String)], sparse0: RDDLabeledPoint, extantColumns: Map[String, Int]): Array[(LabeledPoint, Int, String)] = {
    try {


      logger.info("Realigning data")
      val newColumns: Map[Int, String] = sparse0.columnIndexNameMap()


      val extantTargets: Map[String, Double] = loadTargets()


      lpArray.map {
        case r =>


          val alignedIndiceso: Option[(Seq[Int], Array[Double])] = alignColumnIndicesForLabeledPoint(r._1, newColumns, extantColumns)

          val labelo: Option[Double] = extantTargets.get(sparse0.getTargetName(r._1.label).toLowerCase())


          if (alignedIndiceso.isDefined && labelo.isDefined) {
            val label = labelo.get
            val lp = new LabeledPoint(label, org.apache.spark.ml.linalg.Vectors.sparse(alignedIndiceso.get._1.length, alignedIndiceso.get._1.toArray, alignedIndiceso.get._2))
            val hc = sparse0.lpHash(lp)
            (lp, hc, r._3)
          }
          else {
            logger.info("alignedIndiceso not defined or labelo not defined")
            (r._1, 0, "")
          }

      }.filter(r1 => r1._2 != 0 && r1._3.nonEmpty)

    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  private [cassandra] def getOriginalUid(lp: LabeledPoint, uidColIdx: Int): Try[Double] = {

    val pos: Int = lp.features.toSparse.indices.indexOf(uidColIdx)
    Try(lp.features.toSparse.values(pos))

  }

  private [cassandra] def containsOldUid(extantColumns: Map[String, Int]): Boolean = {
    val olduidCol = CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).uidinsparse.toLowerCase
    extantColumns.keySet.contains(olduidCol)
  }

  private [cassandra] def getOrCreateUUID(uidvalo: Option[String]): String = {
    uidvalo match {
      case x if (!x.isDefined) => UUIDs.timeBased().toString
      case x if (x.get.isEmpty) => UUIDs.timeBased().toString
      case x if (x.get.length < 12) => UUIDs.timeBased().toString
      case _ => uidvalo.get
    }
  }


  private [cassandra] def oldUidColIdx(extantColumns: Map[String, Int]): Int = {
    containsOldUid(extantColumns) match {
      case false => -1
      case true => extantColumns.get(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).uidinsparse.toLowerCase).get
    }
  }

  @throws(classOf[IKodaMLException])
  private def processSparseData(sparse0: RDDLabeledPoint): Boolean = {
    try {
      logger.info("Processing Sparse Data...")

      val lpArray: Array[(LabeledPoint, Int, String)] = sparse0.sparseData().collect()
      logger.info("lpArray size: " + lpArray.size)
      val extantColumns: Map[String, Int] = loadColumns()
      val oldUidIndex = oldUidColIdx(extantColumns)
      logger.info("oldUidIndex " + oldUidIndex)

      import sqlContext.implicits._

      val lpArrayReAligned: Array[(LabeledPoint, Int, String)] = realignSparseData(lpArray, sparse0, extantColumns)


      checksum(lpArrayReAligned, sparse0)
      if(SparseDataInputValidator.validateSparseData(sparse0,lpArrayReAligned,extantColumns)) {

        logger.info("lpArrayReAligned size: " + lpArrayReAligned.size)


        logger.debug("\n----------processSparseData-------------\n")

        val dfOut: DataFrame = lpArrayReAligned.map {
          case r =>

            currentRow = currentRow + 1
            val uuid: String = getOrCreateUUID(Option(r._3))

            val timestamp = System.currentTimeMillis()
            val label = r._1.label

            cacheSentenceToWordRecords(uuid, r._1.features.toSparse.indices)

            oldUidIndex >= 0 match {
              case true =>
                val oldidt: Try[Double] = getOriginalUid(r._1, oldUidColIdx(extantColumns))
                if (oldidt.isSuccess) {
                  cacheOldNewId(oldidt.get, uuid)
                }
                else {
                  logger.warn("\nFailed to get old uid\n" + oldidt.failed.get.getMessage + "\n\n")
                }
              case false =>
            }

            (uuid, label, r._1.features.toSparse.indices, r._1.features.toSparse.values, r._2, timestamp, batchuuid)

        }.toSeq.toDF("uuid", "label", "indices", "values", "hashcode", "timestamp", "batchuuid")

        updateSentenceToWordMap()
        updateOldNewIdMap()

        logger.info(s"Inserting ${dfOut.count} rows to sparseData table")

        dfOut.write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "sparseData", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
          .mode(SaveMode.Append)
          .save()
        logger.info(s"Added ${dfOut.count()} rows to ${pconfig.get(PipelineConfiguration.keyspaceName)}.sparseData")
        true
      }
      else
        {
          logger.warn("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!\nColumn MISMATCH between input data and realigned data " + pconfig.get(PipelineConfiguration.keyspaceName) + ". Deleting batch\n" +
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
          deleteBatchFromTables(batchuuid, pconfig.get(PipelineConfiguration.keyspaceName))
          false
        }


    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def updateColumnMap(lastIdx: Int, newColumns: Set[String]): Unit = {
    try {
      logger.info("Updating columnMap")
      import sqlContext.implicits._
      var idx = lastIdx
      val df = newColumns.map {
        colName =>
          idx = idx + 1
          (cleanColumnName(colName), idx, batchuuid)
      }.toSeq.toDF(Seq("columnName", "columnIndex", "batchuuid"): _*)


      df.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "columnMap", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .mode(SaveMode.Append)
        .save()

      logger.info(s"Added ${df.count()} new columns (i.e., words) to ${pconfig.get(PipelineConfiguration.keyspaceName)}.columnMap")
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }

  private [cassandra] def updateColumnsFromScratch(columnMap: mutable.ListMap[Int, ColumnHeadTuple]): Unit = {
    try {
      import sqlContext.implicits._

      val df: DataFrame = columnMap.map {
        ch => (cleanColumnName(ch._2.stringLabel), ch._2.numericLabel, batchuuid)
      }.toSeq.toDF(Seq("columnName", "columnIndex", "batchuuid"): _*)


      df.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "columnMap", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .mode(SaveMode.Append)
        .save()

      logger.info(s"Added ${df.count()} new columns (i.e., words) to ${pconfig.get(PipelineConfiguration.keyspaceName)}.columnMap")

    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private [cassandra] def updateTargetMap(lastIdx: Double, newTargets: Set[String]): Unit = {
    try {
      logger.info("Updating targetMap")
      import sqlContext.implicits._
      var idx = lastIdx
      val df = newTargets.map {
        targetName =>
          idx = idx + 1
          (targetName.toLowerCase(), idx, batchuuid)
      }.toSeq.toDF(Seq("targetName", "targetIndex", "batchuuid"): _*)

      df.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "targetMap", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .mode(SaveMode.Append)
        .save()
      logger.info(s"Added ${df.count()} rows to ${pconfig.get(PipelineConfiguration.keyspaceName)}.targetMap")
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }

  @throws(classOf[IKodaMLException])
  private def processColumns(finputVsExtantWordDifferences: Map[String, Int] => Set[String]) {
    try {
      var max = (columnMap: Map[String, Int]) => if (columnMap.isEmpty) -1
                                                 else columnMap.values.max
      val extantColumns: Map[String, Int] = loadColumns()


      val maxColumnIndex: Int = max(extantColumns)
      val newColumns: Set[String] = finputVsExtantWordDifferences(extantColumns)

      logger.info(s"\nCreating ${newColumns.size} new columns\nTotal column count is ${extantColumns.size + newColumns.size}")
      updateColumnMap(maxColumnIndex, newColumns)
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def processTargets(finputVsExtantTargetDifferences: Map[String, Double] => Set[String]) {
    try {
      var max = (targetMap: Map[String, Double]) => if (targetMap.isEmpty) -1
                                                    else targetMap.values.max
      val extantTargets: Map[String, Double] = loadTargets()
      val maxTargetIndex: Double = max(extantTargets)
      val newTargets: Set[String] = finputVsExtantTargetDifferences(extantTargets)
      updateTargetMap(maxTargetIndex, newTargets)
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def loadTargets(): Map[String, Double] = {
    getSparkSession()
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "targetMap", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
      .load().collect().map {
      r => (r.getAs[String]("targetName"), r.getAs[Double]("targetIndex"))
    }.toMap
  }

  @throws(classOf[IKodaMLException])
  private def loadColumns(): Map[String, Int] = {


    getSparkSession()
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "columnMap", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
      .load().collect().map {
      r => (r.getAs[String]("columnName"), r.getAs[Int]("columnIndex"))
    }.toMap
  }


  private [cassandra] def splitNewColumns(newCols: Set[String]): Seq[Set[String]] = {

    newCols.grouped(250).toSeq


  }


  @throws
  private val inputVsExtantWordDifferences = (inputColumns: List[ColumnHeadTuple]) => (extantColumns: Map[String, Int]) => {
    try {
      val inputSet: scala.collection.immutable.Set[String] = inputColumns.map {
        cht => cht.stringLabel.toLowerCase()
      }.distinct.toSet

      logger.info(s"s Input Set count: ${inputSet.size}")

      //logger.debug("\n\ninputSet\n"+inputSet.mkString(", "))

      val extantSet: scala.collection.immutable.Set[String] = extantColumns.map {
        e => e._1.toLowerCase()
      }.toSet

      logger.info(s"s Extant Set count: ${extantSet.size}")
      //logger.debug("\n\nextantSet\n"+extantSet.mkString(" * "))
      inputSet.diff(extantSet)
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws
  private val inputVsExtantTargetDifferences = (inputTargets: Map[String, Double]) => (extantTargets: Map[String, Double]) => {
    try {
      val inputSet: scala.collection.immutable.Set[String] = inputTargets.map {
        e => e._1.toLowerCase()
      }.toSet

      val extantSet: scala.collection.immutable.Set[String] = extantTargets.map {
        e => e._1
      }.toSet

      inputSet.diff(extantSet)
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


}
