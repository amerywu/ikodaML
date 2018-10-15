package ikoda.ml.cassandra

import java.util.UUID
import com.datastax.driver.core.ResultSet
import grizzled.slf4j.Logging
import ikoda.IKodaMLException

import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.utilobjects.{DataFrameUtils, DebugObjectTracker, SparkConfProviderWithStreaming}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SaveMode}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try


object SparseSupplementDataInput extends Serializable with SparkConfProviderWithStreaming {



  val uuidnew = udf(() => UUID.randomUUID().toString + "-x")




}

class SparseSupplementDataInput(pconfig: PipelineConfiguration) extends Logging with QueryExecutor {


  private def processUuids(df: DataFrame): DataFrame = {
    try {
      df.schema.fieldNames.contains("uuid") match {
        case true => df
        case false => df.schema.fieldNames.contains(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).uidinsparse) match {
          case true =>
            val oldNewDfo = DataSetCache.get(keyspace+"oldNewIdMap")

            oldNewDfo.isDefined match {
              case true=>
                ///***********8888888888888888888
                  logger.debug ("oldNewDfo.size " + oldNewDfo.get.count)
                logger.debug(DataFrameUtils.showString(oldNewDfo.get))
                logger.debug ("df.size " + df.count)
                logger.debug(DataFrameUtils.showString(df))
                  val dfOut=DataFrameUtils.join(df,oldNewDfo.get,CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).uidinsparse)
                logger.debug ("dfOut.size " + dfOut.count)
                dfOut.drop("dsctimestamp")
              case false =>
                df.withColumn("uuid", SparseSupplementDataInput.uuidnew())
            }
          case false =>
            df.withColumn("uuid", SparseSupplementDataInput.uuidnew())
        }
      }
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }

  val keyspace: String = pconfig.get(PipelineConfiguration.keyspaceName)
  val sqlContext = getSparkSession().sqlContext

  @throws(classOf[IKodaMLException])
  def insertSupplementaryData(pathToCsv: String): Unit = {

    try {
      val df = getSparkSession().read.format("csv").option("header", "true").load(pathToCsv)
      logger.info("loaded csv to dataframe")
      ///************8888888888888888888
      logger.debug("insertSupplementaryData loaded df rowcount "+df.count())




      val dfWithUuid = processUuids(df)
      logger.debug("insertSupplementaryData dfWithUuid rowcount "+dfWithUuid.count())

      logger.debug("\n----------insertSupplementaryData-------------\n")

      //********************888888888888888888888

      logger.debug(dfWithUuid.schema.fieldNames.mkString(","))
      logger.debug(dfWithUuid.collect().take(5).foreach(r => logger.debug(keyspace+"insertSupplementaryData "+r.getAs[String]("uuid") + " "+r.getAs[String](CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).uidinsparse))))


      val seqUuid: Seq[String] = dfWithUuid.rdd.map(r => r.getAs[String]("uuid")).collect()

      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid1", seqUuid))
      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid2", seqUuid))
      logger.debug("mappedUuid1 " + DebugObjectTracker.seqContainsObject("mappedUuid3", seqUuid))
      insertData(dfWithUuid, "sparseSupplement")
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def insertSupplementaryData(pathToCsv: String, tableName: String): Unit = {
    try {
      val df = getSparkSession().read.format("csv").option("header", "true").load(pathToCsv)
      logger.debug("Inserting from data with schema:\n" + df.schema.mkString("\n"))
      insertData(df, tableName)
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }

  @throws(classOf[IKodaMLException])
  def insertSupplementaryData(pathToCsv: String, tableName: String, duplicateCheck: Boolean, addColumnsIfNotExists: Boolean): Unit = {
    try {
      val df = getSparkSession().read.format("csv").option("header", "true").load(pathToCsv)
      logger.debug("Inserting from data with schema:\n" + df.schema.mkString("\n"))
      if (addColumnsIfNotExists) {
        addColumnIfNotExists(df.schema.fieldNames.toSet, tableName, keyspace)
      }
      insertData(df, tableName)
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def insertSupplementaryData(df: DataFrame, validateUUID:Boolean=true): Unit = {
    try {
      ////////////////////////////////888888888888888
      df.take(50).foreach {
        r =>
          logger.info("insertSupplementaryData: " + r.getAs[String]("uuid"))
      }
      insertData(df, "sparseSupplement",validateUUID)
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  def isInt(s: String): Boolean = {
    try {
      s.toInt
      true
    }
    catch {
      case e: Exception => false
    }
  }

  @throws(classOf[IKodaMLException])
  private def insertData(df: DataFrame, tableName: String,validateUUID:Boolean=true): Unit = {
    try {
      import org.apache.spark.sql.functions._

      logger.debug("\ninsertData:\n" + df.schema)
      logger.debug("\ninsertData row count:\n" + df.count())

      if(validateUUID)
        {
          validateUuid()
        }
      val newColumnNames: Seq[String] = df.schema.fieldNames.map(fn => cleanColumnName(fn))
      val dfLowerCase = df.toDF(newColumnNames.toSeq: _*)
      if(!isDuplicateFreeColumnNames(dfLowerCase))
        {
          throw new IKodaMLException("Duplicate column names in incoming data")
        }

      // DataFrameUtils.logRows(dfLowerCase,0.01)
      val sqlC = getSparkSession().sqlContext
      import sqlC.implicits._
      val dff1: DataFrame = dfLowerCase.withColumn("timestamp", when($"uuid".isNull, System.currentTimeMillis()).otherwise(System.currentTimeMillis()))

      val dff2: DataFrame = dff1.filter(r => Option(r.getAs[String]("aa_label")).isDefined).filter(r => Option(r.getAs[String]("uuid")).isDefined)
      logger.debug("\ninsertData dff2:\n" + dff2.schema)
      logger.debug("\ninsertData dff2 count:\n" + dff2.count)
      logger.debug("\ninsertData dff2 count:\n" +DataFrameUtils.showString(dff2,15))




      dff2.write
        .mode(SaveMode.Append)
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName, "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .save()


      logger.info("\n\n--------------------\nInserted data to " + tableName + " in " + keyspace + "\n--------------------\n")
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def validateUuid(): Unit = {
    try {
      val countQuery = "SELECT * FROM " + pconfig.get(PipelineConfiguration.keyspaceName) + ".keyspaceuuid"
      val resultSett: Try[ResultSet] = executeQuery(countQuery)
      if (resultSett.isSuccess) {
        val dbuuidt: Try[String] = Try(resultSett.get.one().getString("uuid"))
        if(dbuuidt.isSuccess) {
          logger.info("\ndbuuid: " + dbuuidt.get + "\ninuuid: " + pconfig.get(PipelineConfiguration.keyspaceUUID))
          require(dbuuidt.get.length > 10)
          require(pconfig.get(PipelineConfiguration.keyspaceUUID).length > 10)
          require(dbuuidt.get == pconfig.get(PipelineConfiguration.keyspaceUUID))
        }
      }
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }

}
