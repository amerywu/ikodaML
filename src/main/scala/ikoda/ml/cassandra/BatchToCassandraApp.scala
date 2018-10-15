package ikoda.ml.cassandra

import grizzled.slf4j.Logging
import ikoda.IKodaMLException

import ikoda.utilobjects.UtilFunctions

/**
  * This is the entry point to BatchToCassandra (b2c) called from spark-submit.
  */
object BatchToCassandraApp extends Logging with UtilFunctions
{



  def main(args: Array[String]): Unit = {
    BatchToCassandra.monitorSparkDirectories()
  }




  @throws(classOf[IKodaMLException])
  def flushStagingDatabase(keyspaceName: String): Unit =
  {
    try
    {
      BatchToCassandra.flushStagingKeySpace(keySpaceCleanName(keyspaceName).get, 0)
    }
    catch
      {
        case e: Exception => logger.error(s"\n\n\n Handled Exception but very serious.\n\n ${e.getMessage}", e)
      }
  }

  @throws(classOf[IKodaMLException])
  def createKeyspace(keyspaceName: String): String =
  {
    try
    {
      logger.info(s"\n---------\nInstructed (from client?) to Create Keyspace If Needed\n---------\n")
      BatchToCassandra.createNewKeyspace(keySpaceCleanName(keyspaceName).get)
    }
    catch
      {
        case e: Exception => logger.error(s"\n\n\n Handled Exception but very serious.\n\n ${e.getMessage}", e)
          "FAILED "+e.getMessage
      }
  }



}



