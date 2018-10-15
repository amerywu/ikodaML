package ikoda.ml.streaming


import java.text.SimpleDateFormat

import grizzled.slf4j.Logging
import ikoda.ml.SparkDirectoryFinder
import ikoda.ml.cassandra.CassandraKeyspaceConfigurationFactory
import ikoda.ml.streaming.MLPCURLModelGenerationDataStream.{getSparkStreamingContext, logger}
import ikoda.utilobjects.SparkConfProviderWithStreaming
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

trait IKodaTextSocketReceiverTrait extends Logging with SparkConfProviderWithStreaming
{
  def disableReceiver(recId:String, port:Int): Unit =
  {
    IKodaTextSocketReceiver.disable(s"$recId:$port")
  }

  def doReceiverStream(ip:String, port:Int): Unit =
  {
    //ssc.checkpoint("./ikoda/cp")
    logger.info("open stream to " + ip + " " + port)

    val ssc = getSparkStreamingContext()
    ssc.addStreamingListener(new IKodaStreamingListener)

    val ikReceiver = new IKodaTextSocketReceiver(ip, port, s"$ip:$port")
    val lines: DStream[String] = ssc.receiverStream(ikReceiver)

    val linesOfSubstance: DStream[String] = lines.filter(line => line.length > 0)
    val lmap: DStream[String] = linesOfSubstance.map
    {
      lraw =>
        lraw match
        {
          case "IKODA_END_STREAM" =>
            System.out.println("End of Data")
            ""
          case x if (x.contains("IKODA_DATA_NAME")) =>
            System.out.println("IKodaTextSocketReceiverTrait IKODA_DATA_NAME "+x.split("=")(1))
            ""
          case x if (x.contains("IKODA_CASSANDRA_KEYSPACE")) =>
            System.out.println("IKodaTextSocketReceiverTrait IKODA_CASSANDRA_KEYSPACE "+x.split("=")(1))
            ""
          case x if (!x.isEmpty) =>
            lraw
          case _ =>
            System.out.println("IKodaTextSocketReceiverTrait-empty")
            ""
        }
    }

    lmap.foreachRDD
    {
      r =>
        if (r.count() > 0)
        {
          logger.info(s"\n---------\nIKodaTextSocketReceiverTrait- RECEIVED: ${r.toString()} first: ${r.first().toString}\n---------\n")
          val rf:RDD[String]=r.filter(s=>s.trim().length>0)

          val keyspaceo=IKodaTextSocketReceiver.keySpaceName(s"$ip:$port")
          val keyspaceuuido=IKodaTextSocketReceiver.keySpaceUUID(s"$ip:$port")
          val fileName=IKodaTextSocketReceiver.dataName(s"$ip:$port")

          if(keyspaceo.isDefined&&keyspaceuuido.isDefined)
          {
            val directory=fileName.contains(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspaceo.get).supplementsuffix) match
            {
              case true=>CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspaceo.get).supplementdir
              case false => CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspaceo.get).dir
            }
            logger.trace(s"\n$directory\n${keyspaceo.get}\n$fileName")
            if(fileName.contains(keyspaceuuido.get))
            {
              rf.saveAsTextFile(s"${directory}/${fileName}")
            }
            else
              {
                rf.saveAsTextFile(s"${directory}/|uuids|${keyspaceuuido.get}|uuide||kss|${keyspaceo.get}|kse|${fileName}")
              }
          }
          else
          {
              rf.saveAsTextFile(s"${"/ikoda/unrecognizedData"}/${fileName}")
          }
        }
        else
        {
          //logger.trace("IKodaTextSocketReceiverTrait - Empty RDD. No data received.")
        }
    }
  }
}
