package ikoda.ml.cassandra

import com.datastax.spark.connector.cql.CassandraConnector
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.SparkConfProviderWithStreaming

/**
  * Drops tables
  * @param pconfig
  */
class SparseTableDropper(pconfig:PipelineConfiguration) extends Logging with QueryExecutor
{
  val keyspace=pconfig.get(PipelineConfiguration.keyspaceName)

  @throws(classOf[IKodaMLException])
  def dropTablesIfExists( sparse0: RDDLabeledPoint): Unit =
  {
    droppedataTable()
    dropColumnTable()
    dropTargetTable()
    dropSentenceToWordMap()
    dropSparseSupplement()
    dropoldNewUidTable()
  }


  @throws(classOf[IKodaMLException])
  private def droppedataTable( )
  {
    val sqlC = getSparkSession().sqlContext

    try
    {
      val df = getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "sparseData", "keyspace" -> keyspace))
        .load()
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("drop TABLE  "+keyspace+".\"sparseData\";")

        logger.info(s"$keyspace.sparseData dropped")
      }
    }
    catch
    {
      case e:Exception=>
        logger.warn(e.getMessage)
    }
  }


  @throws(classOf[IKodaMLException])
  private def dropoldNewUidTable( )
  {
    val sqlC = getSparkSession().sqlContext

    try
    {
      val df = getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "oldnewuid", "keyspace" -> keyspace))
        .load()
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("drop TABLE  "+keyspace+".\"oldnewuid\";")

        logger.info(s"$keyspace.oldnewuid dropped")
      }
    }
    catch
      {
        case e:Exception=>
          logger.warn(e.getMessage)
      }
  }


  @throws(classOf[IKodaMLException])
  private def dropSparseSupplement( )
  {

    try
    {


      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("drop TABLE  "+keyspace+".\"sparseSupplement\";")

        logger.info(s"$keyspace.sparseSupplement dropped")
      }
    }
    catch
    {

      case e:Exception=>
        logger.warn(e.getMessage)

    }

  }



  @throws(classOf[IKodaMLException])
  private def dropColumnTable()
  {

    try
    {

      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("drop TABLE  "+keyspace+".\"columnMap\";")}

      logger.info(s"$keyspace.columnMap dropped")
    }
    catch
    {

      case e:Exception=>
        logger.warn(e.getMessage)

    }

  }

  @throws(classOf[IKodaMLException])
  private def dropTargetTable()
  {
    try
    {


      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("drop TABLE  "+keyspace+".\"targetMap\";")}

      logger.info(s"$keyspace.targetMap dropped")
    }
    catch
    {

      case e: Exception =>
        logger.warn(e.getMessage)

    }

  }

  @throws(classOf[IKodaMLException])
  private def dropSentenceToWordMap()
  {
    val sqlC = getSparkSession().sqlContext
    try
    {
      CassandraConnector(getSparkSession().sparkContext.getConf).withSessionDo { session =>
        session.execute("drop TABLE  "+keyspace+".\"sentenceToWordMap\";")}
      logger.info(s"$keyspace.sentenceToWordMap dropped")
    }
    catch
    {
      case e:Exception=>
        logger.warn(e.getMessage)
    }
  }

}
