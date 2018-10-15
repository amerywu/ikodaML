package ikoda.ml.cassandra

import ikoda.IKodaMLException

import ikoda.utils.TicToc
import com.datastax.spark.connector._

/**
  * Deletes an incoming batch from Cassandra. WARNING, will create tombstones
  */
trait SparseBatchDelete extends QueryExecutor
{


  protected def deleteBatchFromTables(batchuuid:String, keyspaceName:String): Unit =
  {
    try
    {
      val tt:TicToc=new TicToc()
      logger.warn(tt.tic("\n!!!!!!!!!!!!!!\n!!Deleting batch due to potential corruption\n!!!!!!!!!!!!!!\n!!",60000))

      getSparkSession().sparkContext.cassandraTable(keyspaceName, "sparseData")
        .where("batchuuid = '"+batchuuid+"'")
        .deleteFromCassandra(keyspaceName, "sparseData")

      getSparkSession().sparkContext.cassandraTable(keyspaceName, "sentenceToWordMap")
        .where("batchuuid = '"+batchuuid+"'")
        .deleteFromCassandra(keyspaceName, "sentenceToWordMap")

      getSparkSession().sparkContext.cassandraTable(keyspaceName, "columnMap")
        .where("batchuuid = '"+batchuuid+"'")
        .deleteFromCassandra(keyspaceName, "columnMap")

      getSparkSession().sparkContext.cassandraTable(keyspaceName, "targetMap")
        .where("batchuuid = '"+batchuuid+"'")
        .deleteFromCassandra(keyspaceName, "targetMap")


      logger.warn(tt.toc("\n!!!!!!!!!!!!!!\n!!Deleting batch due to potential corruption\n!!!!!!!!!!!!!!\n!!"))

    }
    catch
    {
      case e:Exception=> throw new IKodaMLException(e.getMessage,e)
    }
  }


}
