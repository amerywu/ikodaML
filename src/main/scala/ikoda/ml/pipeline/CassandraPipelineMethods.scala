package ikoda.ml.pipeline

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.cassandra.{SparseDataToRDDLabeledPoint, _}
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.{SimpleLog, SparkConfProviderWithStreaming}
import ikoda.utils.TicToc
import org.apache.spark.sql.DataFrame

/**
  * CassandraPipelineMethods handles pipeline methods that connect to Cassandra.
  *
  * Each method in this trait encapsulate a specific data pipeline operation.
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
  *
  */
trait CassandraPipelineMethods extends Logging with SimpleLog  with SparkConfProviderWithStreaming with PipelineFunctionTypes
{

  type FTDataReductionProcessCassandra =
    (PipelineConfiguration => (Option[RDDLabeledPoint] => Option[RDDLabeledPoint]))


  @throws
  val createCassandraTables:FTDataReductionProcessCassandra = (pconfig:PipelineConfiguration) => (osparse:Option[RDDLabeledPoint]) =>
  {
    try
    {
      addLine("***createCassandraTables")
      if(osparse.isDefined)
      {
        val cassandraModel:SparseModelMaker=new SparseModelMaker(pconfig)
        cassandraModel.createTablesIfNotExist(osparse.get)
      }
      else
      {
        logger.warn("No dataset provided. Aborting")
      }
      osparse
    }
    catch
    {
      case e:Exception => logger.error(e.getMessage,e)
        throw e
    }
  }


  @throws
  val loadFromCassandra:FTDataReductionProcessCassandra = (pconfig:PipelineConfiguration) => (osparse:Option[RDDLabeledPoint]) =>
  {
    try
    {
        addLine("Loading Data from Cassandra")
        val cassandraLoader:SparseDataToRDDLabeledPoint=new SparseDataToRDDLabeledPoint(pconfig)
        val sparseOut= cassandraLoader.loadFromCassandra

        sparseOut.validateColumnCount()

        pconfig.config(PipelineConfiguration.printStageName,"loadFromCassandra")

        Some(sparseOut)
    }
    catch
    {
      case e:Exception => logger.error(e.getMessage,e)
        None
    }
  }


  @throws
  val saveToCassandra:FTDataReductionProcessCassandra = (pconfig:PipelineConfiguration) => (osparse:Option[RDDLabeledPoint]) =>
  {
    try
    {

      addLine("***saveToCassandra")
      if(osparse.isDefined)
      {
        val cassandraModel:SparseModelMaker=new SparseModelMaker(pconfig)
        cassandraModel.createTablesIfNotExist(osparse.get)

        val dataInput:SparseDataInput=new SparseDataInput(pconfig)
        dataInput.insertAllData(osparse.get)
      }
      else
      {
        logger.warn("No dataset provided. Aborting")
      }
      osparse
    }
    catch
    {
      case e:Exception => logger.error(e.getMessage,e)
        throw e
    }
  }





  @throws
  val truncateSparseTables:FTDataReductionProcessCassandra = (pconfig:PipelineConfiguration) => (osparse:Option[RDDLabeledPoint]) =>
  {
    try
    {

      addLine("***truncateSparseTables")


        val dataInput:SparseTableTruncator=new SparseTableTruncator(pconfig)
       // dataInput.truncateTables(osparse.get)



      osparse



    }
    catch
    {
      case e:Exception => logger.error(e.getMessage,e)
        throw e
    }
  }


  @throws
  val dropSparseTables:FTDataReductionProcessCassandra = (pconfig:PipelineConfiguration) => (osparse:Option[RDDLabeledPoint]) =>
  {
    try
    {

      addLine("***dropSparseTables")


      val dataInput:SparseTableDropper=new SparseTableDropper(pconfig)
      dataInput.dropTablesIfExists(osparse.get)

      osparse
    }
    catch
    {
      case e:Exception => logger.error(e.getMessage,e)
        throw e
    }
  }


  val loadSparseSupplement: FTDataReductionProcessDataFrame= (pconfig:PipelineConfiguration) => (osparse:Option[DataFrame]) =>{
    try {
      logger.info("loadSparseSupplement")
      val ssr: SparseSupplementRetriever = new SparseSupplementRetriever(pconfig)
      Some(ssr.loadSparseSupplement())
    }
    catch {
      case e: Exception => throw new IKodaMLException("loadSparseSupplement: " + e.getLocalizedMessage, e)
        None
    }
  }

  
}



