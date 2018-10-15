package ikoda.ml.cassandra

import com.datastax.driver.core.ResultSet
import grizzled.slf4j.Logging
import ikoda.ml.cassandra.BatchToCassandraFlushStaging.{executeQuery, logger}
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.UtilFunctions
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.util.Try

trait BatchToCassandraTrait extends Logging with UtilFunctions with QueryExecutor
{


  protected  val sparseSupplementSet: mutable.Set[String] = mutable.Set[String]()
  protected val filesOlderThan:Long=180000
  protected val sleepTime:Long=60000


  protected def registerKeyspaceForSparse(ks: String): Unit =
  {
    SparseModelMaker.keyspaceExists(ks) match
    {
      case false =>
        logger.info(s"\n----------\nRegistering $ks\n----------\n")
        val pconfig: PipelineConfiguration = new PipelineConfiguration
        pconfig.config(PipelineConfiguration.keyspaceName, ks)
        val smm: SparseModelMaker = new SparseModelMaker(pconfig)
        val sparse0: RDDLabeledPoint = new RDDLabeledPoint
        smm.createTablesIfNotExist(sparse0.dummyRow().get)

      case true =>
    }
  }


  protected def countQuery(keyspaceName:String): Try[ResultSet] =
  {
    synchronized
    {
      val countQuery = "SELECT COUNT(*) FROM " + keyspaceName + ".\"sparseData\""
      executeQuery(countQuery)
    }
  }

  protected def countQuery(keyspaceName:String, tableName:String): Try[ResultSet] =
  {
    synchronized
    {
      val countQuery = "SELECT COUNT(*) FROM " + keyspaceName + ".\"+tableName+\""
      executeQuery(countQuery)
    }
  }

  protected def simpleCountQuery(keyspaceName:String, tableName:String): Long =
  {
    synchronized
    {
      val countQuery = "SELECT COUNT(*) FROM " + keyspaceName + ".\""+tableName+"\""
      val rs=executeQuery(countQuery)
      rs.isSuccess match
      {
        case true => val t=Try(rs.get.one().getLong(0))
          t.getOrElse(0)
        case false =>
          logger.warn("simpleCountQuery "+rs.failed.get.getMessage)
          -1
      }
    }
  }

  protected def keyspaceExists(ks: String): Boolean =
  {
    SparseModelMaker.keyspaceExists(ks)
  }

  protected def truncateKeyspace(keyspaceName: String): Unit =
  {
    try
    {
      logger.info("Truncating "+keyspaceName)
      val pconfig: PipelineConfiguration = new PipelineConfiguration
      pconfig.config(PipelineConfiguration.keyspaceName, keyspaceName)
      val stt: SparseTableTruncator = new SparseTableTruncator(pconfig)
      stt.truncateTables()
    }
    catch
      {
        case e: Exception => logger.warn("\n\nSerious but handled\n" + e.getMessage, e)
      }
  }
}
