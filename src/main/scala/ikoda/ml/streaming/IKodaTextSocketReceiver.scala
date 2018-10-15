package ikoda.ml.streaming


import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.utils.StreamingConstants
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



object IKodaTextSocketReceiver extends Logging
{
  val receiverMap: mutable.HashMap[String, IKodaTextSocketReceiverStatus] = mutable.HashMap[String,
    IKodaTextSocketReceiverStatus]()

  def clear(): Unit =
  {
    receiverMap.clear()

  }

  def killAll(): Unit =
  {
    val rSeq:Seq[(String,IKodaTextSocketReceiverStatus)]=receiverMap.map
    {
      case (k,r)=> (k,r)
    }.toSeq

    rSeq.foreach
    {
      e=> receiverMap.put(
        e._1, new IKodaTextSocketReceiverStatus(
          e._1, receiverMap.get(e._1).get.enabled, receiverMap.get(e._1).get.activeReceiving,
          receiverMap.get(e._1).get.dataName,  receiverMap.get(e._1).get.streamId, receiverMap.get(e._1).get.keySpaceName,receiverMap.get(e._1).get.keySpaceUUID,true
        ))
    }
  }

  def kill(recId: String): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      receiverMap.get(recId).get.kill
    }
    else
    {
      logger.warn(s"UNKNOWNRECEIVER_${recId}")
      true
    }
  }








  def confirmStreamId(recId: String, sid: Int): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {

      if (sid != receiverMap.get(recId).get.streamId)
      {
        logger.warn(s"Changed streamId: "+receiverMap.get(recId).get.streamId)
        receiverMap.put(
          recId, new IKodaTextSocketReceiverStatus(
            recId, receiverMap.get(recId).get.enabled, receiverMap.get(recId).get.activeReceiving,
            receiverMap.get(recId).get.dataName,  sid, receiverMap.get(recId).get.keySpaceName,receiverMap.get(recId).get.keySpaceUUID,false
          )
        )
      }

      true

    }
    else
    {
      false
    }
  }




  def restartReceiver(recId: String): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      logger.info("RestartReceiver: Found existing receiver for " + recId)

      receiverMap.put(
        recId, new IKodaTextSocketReceiverStatus(
          recId, true, true, receiverMap.get(recId).get.dataName,
          receiverMap.get(recId).get.streamId, receiverMap.get(recId).get.keySpaceName,receiverMap.get(recId).get.keySpaceUUID,false
        )
      )

      true

    }
    else
    {
      false
    }
  }

  def enableReceiver(recId: String): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      logger.info("Enabling receiver " + recId)

      receiverMap.put(
        recId, new IKodaTextSocketReceiverStatus(
          recId, true, receiverMap.get(recId).get.activeReceiving, receiverMap.get(recId).get.dataName,
          receiverMap.get(recId).get.streamId, receiverMap.get(recId).get.keySpaceName,receiverMap.get(recId).get.keySpaceUUID,false
        )
      )
      true
    }
    else
    {
      false
    }
  }

  def isActiveReceiving(streamId:Int): Boolean =
  {
    val recIdo = getRecId(streamId)
    if (recIdo.isDefined)
    {
      isActiveReceiving(recIdo.get)
    }
    else
    {
      logger.warn(s"No receiver matches streamId $streamId")
      false
    }
  }

  def isActiveReceiving(recId: String): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      receiverMap.get(recId).get.activeReceiving
    }
    else
    {
      false
    }
  }

  def isEnabled(recId: String): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      receiverMap.get(recId).get.enabled
    }
    else
    {
      false
    }
  }


  def getRecId(streamId: Int): Option[String] =
  {
    val o: Option[(String, IKodaTextSocketReceiverStatus)] = receiverMap.find(e => e._2.streamId == streamId)
    if (o.isDefined)
    {
      Some(o.get._2.receiverId)
    }
    else
    {
      None
    }
  }

  def isEnabled(streamId: Int): Boolean =
  {
    val recIdo = getRecId(streamId)
    if (recIdo.isDefined)
    {
      isEnabled(recIdo.get)
    }
    else
    {
      logger.warn(s"No receiver matches streamId $streamId")
      false
    }
  }

  def dataName(recId: String): String =
  {
    if (receiverMap.get(recId).isDefined)
    {
      receiverMap.get(recId).get.dataName
    }
    else
    {
      s"UNKNOWNRECEIVER_${recId}"
    }
  }

  def keySpaceName(recId: String): Option[String] =
  {
    if (receiverMap.get(recId).isDefined)
    {
      receiverMap.get(recId).get.keySpaceName
    }
    else
    {
      logger.warn(s"UNKNOWNRECEIVER_${recId}")
      None
    }
  }

  def keySpaceUUID(recId: String): Option[String] =
  {
    if (receiverMap.get(recId).isDefined)
    {
      receiverMap.get(recId).get.keySpaceUUID
    }
    else
    {
      logger.warn(s"UNKNOWNRECEIVER_${recId}")
      None
    }
  }


  def setActiveReceiving(streamId: Int, receiving: Boolean): Boolean =
  {
    val o = getRecId(streamId)
    if (o.isDefined)
    {
      setActiveReceiving(o.get, receiving)
    }
    else
    {
      false
    }

  }

  def setActiveReceiving(recId: String, changed: Boolean): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      logger.info("Status Changed " + recId)

      receiverMap.put(
        recId, new IKodaTextSocketReceiverStatus(
          recId, receiverMap.get(recId).get.enabled, changed, receiverMap.get(recId).get.dataName,
           receiverMap.get(recId).get.streamId, receiverMap.get(recId).get.keySpaceName,receiverMap.get(recId).get.keySpaceUUID,false
        )
      )

      true

    }
    else
    {
      false
    }
  }

  def setDataName(recId: String, dname: String): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      logger.info("New data for  " + recId + " " + dname)

      receiverMap.put(
        recId, new IKodaTextSocketReceiverStatus(
          recId, receiverMap.get(recId).get.enabled, receiverMap.get(recId).get.activeReceiving, dname,
           receiverMap.get(recId).get.streamId, receiverMap.get(recId).get.keySpaceName,receiverMap.get(recId).get.keySpaceUUID,false
        )
      )

      true

    }
    else
    {
      logger.warn(s"No receiver matches streamId $recId")
      false
    }
  }


  def setKeySpaceName(recId: String, ksname: Option[String]): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      logger.info("New ks for  " + recId + " " + ksname)

      receiverMap.put(
        recId, new IKodaTextSocketReceiverStatus(
          recId, receiverMap.get(recId).get.enabled, receiverMap.get(recId).get.activeReceiving, receiverMap.get(recId).get.dataName,
          receiverMap.get(recId).get.streamId, ksname,receiverMap.get(recId).get.keySpaceUUID,false
        )
      )

      true

    }
    else
    {
      logger.warn(s"No receiver matches streamId $recId")
      false
    }
  }


  def setKeySpaceUUID(recId: String, uuid: Option[String]): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      logger.info("New uuid for  " + recId + " " + uuid)

      receiverMap.put(
        recId, new IKodaTextSocketReceiverStatus(
          recId, receiverMap.get(recId).get.enabled, receiverMap.get(recId).get.activeReceiving, receiverMap.get(recId).get.dataName,
          receiverMap.get(recId).get.streamId, receiverMap.get(recId).get.keySpaceName,uuid,false
        )
      )

      true

    }
    else
    {
      logger.warn(s"No receiver matches streamId $recId")
      false
    }
  }


  def disable(sid: Int): Boolean =
  {
    val recIdo = getRecId(sid)
    if (recIdo.isDefined)
    {
      disable(recIdo.get)
    }
    else
    {
      logger.warn(s"No receiver matches streamId $sid")
      false
    }
  }


  def disable(recId: String): Boolean =
  {
    if (receiverMap.get(recId).isDefined)
    {
      logger.info(s"$recId Disabling existing receiver")
      receiverMap.put(
        recId, new IKodaTextSocketReceiverStatus(
          recId, false, receiverMap.get(recId).get.activeReceiving, receiverMap.get(recId).get.dataName,
           receiverMap.get(recId).get.streamId, receiverMap.get(recId).get.keySpaceName,receiverMap.get(recId).get.keySpaceUUID,false
        )
      )
      true
    }
    else
    {
      false
    }
  }


}

class IKodaTextSocketReceiver(host: String, port: Int, receiverId: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging
{

  val buffer:ArrayBuffer[String]= ArrayBuffer()
  val receiverName = s"$host:$port"

  var monitorReceiver = true

  IKodaTextSocketReceiver.receiverMap.put(
    receiverId, new IKodaTextSocketReceiverStatus(receiverId, true, false, "NA",  this.streamId, None,None,false)
  )
  startMonitorStatus


  def startMonitorStatus(): Unit =
  {
    logger.info ("\n\nSTARTING IKodaTextSocketReceiver: "+receiverId+"\n\n")
    new Thread(receiverName + "Monitor")
    {
      override def run()
      {
        logger.info(s"Starting receiverMonitor ${receiverName} Monitor")
        monitorStatus()
      }
    }.start()
  }

  def logStatus(): String =
  {

    s"$receiverName id: ${this.streamId} "+s"$receiverName batch processing status: ${IKodaTextSocketReceiver.isActiveReceiving(receiverName)}"

  }

  @throws(classOf[IKodaMLException])
  def monitorStatus(): Unit =
  {
    try
    {
      while (monitorReceiver)
      {
        IKodaTextSocketReceiver.confirmStreamId(receiverName, this.streamId)
        logger.trace(logStatus())
        Thread.sleep(6000)
        if(IKodaTextSocketReceiver.kill(receiverName))
          {
            monitorReceiver=false
          }
      }
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  def onStart()
  {
    // Start the thread that receives data over a connection

    logger.info(s"Starting IKodaTextSocketReceiver $receiverName")
    if (!super.isStopped())
    {
      new Thread(receiverName)
      {
        override def run()
        {
          receive()
        }
      }.start()
    }
    else
    {
      logger.warn("Restarting after stop set")
    }
  }

  def onStop()
  {
    monitorReceiver = false
    logger.warn(s"$receiverName has stopped.")

  }


  /** Create a socket connection and receive data until receiver is stopped */
  private def receive()
  {

    var socket: Socket = null
    var userInput: String = null

    try
    {
      logger.info(s"Connecting to $host : $port")




      socket = new Socket(host, port)
      logger.info(s"Connected to $host : $port")
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)
      )
      userInput = reader.readLine()
      while (!isStopped && userInput != null)
      {

        buffer += userInput
        userInput = reader.readLine()


        if (userInput.contains(StreamingConstants.IKODA_END_STREAM))
        {
          logger.info(s"$receiverName: ${StreamingConstants.IKODA_END_STREAM}")
          //IKodaTextSocketReceiver.disable(receiverName)

          logger.info(s"Storing ${buffer.size} lines")
          store(buffer)
          buffer.clear()
          logger.info(s"\n---------\n$receiverName: Data fully received ${IKodaTextSocketReceiver.dataName(receiverName)}\n---------\n")

        }
      }
      reader.close()
      socket.close()
      logger.info("Stopped receiving")
      restart("Trying to connect again")
    }
    catch
    {
      case e: java.net.ConnectException =>

        logger.error(s"$receiverName ${e.getMessage}")
        restart(s"$receiverName No connection to $host : $port " + e.getMessage)

      case t: Throwable =>
        logger.error(s"$receiverName ${t.getMessage}")

        restart(s"$receiverName No connection to $host : $port" + t.getMessage)
    }
  }





}
