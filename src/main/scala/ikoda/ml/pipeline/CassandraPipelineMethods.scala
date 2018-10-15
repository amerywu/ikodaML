package ikoda.ml.pipeline

import grizzled.slf4j.Logging
import ikoda.ml.cassandra.{SparseDataToRDDLabeledPoint, _}
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.{SimpleLog, SparkConfProviderWithStreaming}
import ikoda.utils.TicToc


trait CassandraPipelineMethods extends Logging with SimpleLog  with SparkConfProviderWithStreaming
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

  
}



