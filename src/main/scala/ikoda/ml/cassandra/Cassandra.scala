package ikoda.ml.cassandra

import com.datastax.spark.connector._
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.utilobjects.SparkConfProviderWithStreaming


object Cassandra extends SparkConfProviderWithStreaming with Logging
{

  @throws(classOf[IKodaMLException])
  def testConnect(): Unit =
  {
    try
    {
      val rdd = spark.sparkContext.cassandraTable("k1", "person")
      logger.info("testConnect "+rdd.count)
      rdd.foreach
      {
        r=>logger.debug(s"\n\n${r.columnValues.mkString("  -  ")}\n\n")
      }
      val collection = spark.sparkContext.parallelize(Seq((String.valueOf(System.currentTimeMillis()),"name1","stone","wer@safdfsa")))
      collection.saveToCassandra("k1", "person", SomeColumns("id", "name","surname","email"))

      logger.info("\nCassandra Connection Confirmed\n")

    }
    catch
      {
        case e:Exception=>logger.error(e.getMessage,e)
          throw IKodaMLException(e.getMessage,e)
      }
  }
}
