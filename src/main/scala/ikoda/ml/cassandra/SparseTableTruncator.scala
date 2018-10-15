package ikoda.ml.cassandra


import com.datastax.spark.connector.cql.CassandraConnector
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.utilobjects.{ SparkConfProviderWithStreaming}

/**
  *
  * Truncates tables. Used to truncate staging tables after flush
  * @param pconfig
  */
class SparseTableTruncator(pconfig:PipelineConfiguration) extends Logging with SparkConfProviderWithStreaming
{
  val keyspace=pconfig.get(PipelineConfiguration.keyspaceName)

  @throws(classOf[IKodaMLException])
  def truncateTables(): Unit =
  {
    truncateDataTable()
    truncateColumnTable()
    truncateTargetTable()
    truncateSentenceToWordMap()
    truncateSparseSupplement()
    //truncateOldNewUid()
  }

  @throws(classOf[IKodaMLException])
  private def truncateDataTable()
  {
    try
    {
      logger.debug("truncateDataTable")
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("TRUNCATE TABLE  "+keyspace+".\"sparseData\";")
      }
    }
    catch
    {
      case e:Exception=>
        logger.warn(e.getMessage)
    }
  }



  @throws(classOf[IKodaMLException])
  private def truncateColumnTable()
  {
    try
    {
      logger.debug("truncateColumnTable")
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute(s"TRUNCATE TABLE  "+keyspace+".\"columnMap\";")}
      logger.info(s"$keyspace.columnMap truncated")
    }
    catch
    {
      case e:Exception=>
        logger.warn(e.getMessage,e)
    }
  }

  @throws(classOf[IKodaMLException])
  private def truncateTargetTable()
  {
    try
    {
      logger.debug("truncateTargetTable")
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("TRUNCATE TABLE  "+keyspace+".\"targetMap\";")}
      logger.info(s"$keyspace.targetMap truncated")
    }
    catch
    {

      case e: Exception =>
        logger.warn(e.getMessage)

    }
  }


  @throws(classOf[IKodaMLException])
  private def truncateSentenceToWordMap()
  {
    try
    {
      logger.debug("truncateSentenceToWordMap")
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("TRUNCATE TABLE  "+keyspace+".\"sentenceToWordMap\";")}
      logger.info(s"$keyspace.sentenceToWordMap truncated")
    }
    catch
    {
      case e:Exception=>
        logger.warn(e.getMessage)
    }
  }

  @throws(classOf[IKodaMLException])
  private def truncateOldNewUid()
  {
    try
    {
      logger.debug("truncateSentenceToWordMap")
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("TRUNCATE TABLE  "+keyspace+".\"oldnewuid\";")}
      logger.info(s"$keyspace.oldnewuid truncated")
    }
    catch
      {
        case e:Exception=>
          logger.warn(e.getMessage)
      }
  }

  @throws(classOf[IKodaMLException])
  def truncateSparseSupplement()
  {
    try
    {
      logger.debug("truncateSparseSupplement")
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("TRUNCATE TABLE  "+keyspace+".\"sparseSupplement\";")}
      logger.info(s"$keyspace.sparseSupplement truncated")

    }
    catch
    {
      case e:Exception=>
        logger.warn(e.getMessage)
    }
  }

}
