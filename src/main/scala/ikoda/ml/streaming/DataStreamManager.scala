package ikoda.ml.streaming


import java.io.File

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.SparkDirectoryFinder
import ikoda.ml.cassandra.CassandraKeyspaceConfigurationFactory
import ikoda.ml.predictions.MLServiceUtils
import ikoda.utilobjects.{SparkConfProviderWithStreaming, UtilFunctions}
import ikoda.utils.ProcessStatus
import org.apache.spark.streaming.StreamingContextState

/**
  * Manages data streams. This is not thread safe. It manages one stream at a time.
  *
  * - Starts a new StreamingContext if required.
  *
  * - Instantiates a new receiver if required.
  *
  * - Listens for new data batches.
  *
  * - Passes the data name to the [[IKodaTextSocketReceiver]] object
  *
  * - Passes a "wait" message to remote connections if the receiver is still actively receiving data.
  *
  */
object DataStreamManager extends Logging with MLServiceUtils with SparkConfProviderWithStreaming with UtilFunctions
{

  var sscStarted: Boolean = false
  var sscRestartActivating:Boolean =false

  var countEmptyCycles = 0
  var newDataStartedTimeInMillis: Long = 0L;

  var dataStreamCount=0



  def stop(): Unit =
  {

    forceSSCStop

  }








  private def archiveProcessedData(ks: String): Unit =
  {
    try
    {
      val ckc=CassandraKeyspaceConfigurationFactory.keyspaceConfig(ks)
      ckc.archive match
      {
        case false =>
        case true =>
          val currentDir=ckc.dir
          val archiveDir=ckc.archivedir
          val dirsSeq:Seq[org.apache.hadoop.fs.Path]=SparkDirectoryFinder.pathsOlderThan(System.currentTimeMillis()-240000,currentDir)
          dirsSeq.foreach(p => SparkDirectoryFinder.renamePartition(p,new org.apache.hadoop.fs.Path(archiveDir+File.separator+p.getName)))
      }
    }
    catch
    {
      case e: Exception => logger.error(s"\n\n\n Handled Exception but very serious.\n\n ${e.getMessage}", e)
    }
  }



  private def forceSSCStop: Unit =
  {
    logger.warn("\n\nFORCING STREAMING CONTEXT STOP\n\n")
    sscStarted=false

    IKodaTextSocketReceiver.killAll()

    getSparkStreamingContext().stop(true, true)
    logger.info("SparkStreamingContext Awaiting Termination")
    getSparkStreamingContext().awaitTermination()
    logger.info("SparkStreamingContext Terminated")
    Thread.sleep(10000)
    IKodaTextSocketReceiver.clear()
    killSparkStreamingContext
    logger.info("forceSSCStop completed")



  }

  private def sscRestart(): Unit =
  {

    ProcessStatus.incrementStatus("Stream: FORCING STREAMING CONTEXT RESTART")

    startStream

  }



  private def forceContextRestart(): Unit =
  {


    forceSSCStop
    logger.warn("\n\nSTOPPED STREAMING CONTEXT\n\n")
    sscRestart

//////////////////////





  }

  @throws(classOf[IKodaMLException])
  def newDataSet(ip: String, port: String, dataName: String, keySpaceName: String, keySpaceUUID:String): String =
  {
    try
    {
      synchronized {
        logger.info(s"\n>>>>>>>>>>>>>>>>>>>>\nNew Data Set $ip:$port $dataName $keySpaceName $keySpaceUUID\n>>>>>>>>>>>>>>>>>>>>\n")
        sscStarted match {

          case false =>
            if(getSparkStreamingContext().getState().equals(StreamingContextState.INITIALIZED))
              {
                if (newDataStartedTimeInMillis == 0)
                  {
                    newDataStartedTimeInMillis = System.currentTimeMillis()
                  }

              }
            logger.info("Waiting for initialization to complete")
            "WAIT"

          case true =>
            IKodaTextSocketReceiver.isActiveReceiving(s"$ip:$port") match
              {
                case true =>
                  if (System.currentTimeMillis() - newDataStartedTimeInMillis > 120000)
                             {
                                IKodaTextSocketReceiver.setActiveReceiving(s"$ip:$port", false)
                               logger.warn("\n\nForcing IKodaTextSocketReceiver activation")
                                if (System.currentTimeMillis() - newDataStartedTimeInMillis > 300000)
                                {
                                  forceContextRestart()
                                }
                              }
                  ProcessStatus.averageOver("Stream: Average Delay (last 10)",10,System.currentTimeMillis() - newDataStartedTimeInMillis)
                  logger.info(s"Receiver $ip:$port busy. Sending WAIT signal. Millis since last success: "+(System.currentTimeMillis() - newDataStartedTimeInMillis))
                  "WAIT";

                case false =>
                  newDataStartedTimeInMillis = System.currentTimeMillis()
                  dataStreamCount = dataStreamCount+1

                   IKodaTextSocketReceiver.setActiveReceiving(s"$ip:$port", true)

                  logger.info(s"\n---------\n$dataStreamCount batches received. New data set: $dataName\n---------\n")
                  ProcessStatus.incrementStatus("Stream: Files received")
                  archiveProcessedData(keySpaceCleanName(keySpaceName).getOrElse("defaultks"))
                  logger.trace("setting " + dataName)
                  IKodaTextSocketReceiver.setDataName(s"$ip:$port", dataName)
                  IKodaTextSocketReceiver.setKeySpaceName(s"$ip:$port", keySpaceCleanName(keySpaceName))
                  IKodaTextSocketReceiver.setKeySpaceUUID(s"$ip:$port", Option(keySpaceUUID))
                  IKodaTextSocketReceiver.restartReceiver(s"$ip:$port")
                  logStatus
                  "SUCCESS"
              }
        }

      }

    }
    catch
    {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  def logStatus: Unit =
  {
    if(dataStreamCount % 20 == 0)
      {
        logger.info(ProcessStatus.print())
      }
  }


  def startStream(): String =
  {
    synchronized {
      try {
        if (!sscStarted) {
          if (!sscRestartActivating) {
            sscRestartActivating=true
            SparkDirectoryFinder.info
            logger.warn("\n\nSTARTING STREAMING CONTEXT\n\n")

            val urls = ConfigFactory.load("streaming").getString("streaming.friends.urls")
            val ports = ConfigFactory.load("streaming").getString("streaming.friends.ports")
            logger.info("Opening urls: " + urls)
            logger.info("Opening ports: " + ports)
            val urlList = urls.split(",").toList
            val portList = ports.split(",").toList

            val thread = new Thread {
              override def run {
                initializeReceivers(urlList, portList)
              }
            }
            thread.start
          }
        }
        else {
          logger.trace("Nothing to do. Receivers initialized already.")
        }
        "SUCCESS"
      }
      catch {
        case e: Exception =>
          logger.error(e.getMessage, e)
          e.getMessage
      }
    }
  }


  @throws(classOf[IKodaMLException])
  private def initializeReceivers(iplist: List[String], plist: List[String]): Unit =
  {
    val ssc = getSparkStreamingContext()
    iplist.foreach
    {
      ip =>

        plist.toSeq.map(s => s.toInt).foreach
        {
          port =>
            try
            {
              System.out.println("doStream " + port + "\n")
              if (!IKodaTextSocketReceiver.restartReceiver(s"$ip:$port"))
              {
                val service: IKodaTextSocketReceiverService = new IKodaTextSocketReceiverService()
                service.doReceiverStream(ip, port)
              }
            }
            catch
            {
              case e: Exception =>
                logger.error(e.getMessage, e)
                throw new IKodaMLException(e.getMessage, e)
            }
        }
        logger.info("Started \n" + plist.mkString("\n"))
    }
    sscStarted = true

    ssc.start()
    sscRestartActivating=false
    ssc.awaitTermination()
  }


}
