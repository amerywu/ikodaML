package ikoda.ml.streaming

import grizzled.slf4j.Logging
import ikoda.utils.ProcessStatus
import org.apache.spark.streaming.scheduler._

import scala.collection.mutable

/**
  * Monitors and logs streaming activity
  */
class IKodaStreamingListener extends StreamingListener with Logging
{

  val statusMap:mutable.HashMap[Int,Boolean]=new mutable.HashMap[Int,Boolean]()

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit =
  {
    //logger.info("\n\n\n  Listener says Receiver started\n\n\n\n")
    //  logger.info(receiverStarted.receiverInfo.name)
     // logger.info(receiverStarted.receiverInfo.streamId)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =
  {
    batchCompleted.batchInfo.streamIdToInputInfo.foreach
    {
      e =>
        if (e._2.numRecords > 0)
        {
          logger.info("  Listener saysbatchCompleted\n")
          logger.info(s"id: ${e._1}")
          logger.info(s"inputStreamId: ${e._2.inputStreamId}")
          logger.info(s"numRecords: ${e._2.numRecords}")
          ProcessStatus.incrementStatus("Stream: Batches Completed")
        }
    }

  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit =
  {


    batchSubmitted.batchInfo.streamIdToInputInfo.foreach
    {
      e =>
        if (e._2.numRecords > 0)
        {
          logger.info("Listener says batchSubmitted")
          ProcessStatus.incrementStatus("Stream: Batches Submitted")

          //logger.info(s"id: ${e._1}")
          //logger.info(IKodaTextSocketReceiver.getRecId(e._1))
          //logger.info(s"inputStreamId: ${e._2.inputStreamId}")
          //logger.info(s"numRecords: ${e._2.numRecords}")
        }
    }
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit =
  {
    /** logger.info("  Listener says batchStarted\n") */
    batchStarted.batchInfo.streamIdToInputInfo.foreach
    {
      e =>
        if (e._2.numRecords > 0)
        {
          logger.info("  Listener says batchStarted\n")
          //logger.info(s"id: ${e._1}")
          //logger.info(s"inputStreamId: ${e._2.inputStreamId}")
          logger.info(s"numRecords: ${e._2.numRecords}")
          IKodaTextSocketReceiver.setActiveReceiving(e._1,true)
          statusMap.put(e._2.inputStreamId,true)

        }
    }
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit =
  {
    val bo=statusMap.get(outputOperationCompleted.outputOperationInfo.id)
    if(bo.isDefined)
    {
      if (true==bo.get)
      {
        logger.info("  Listener says outputOperationCompleted\n")
        logger.info(IKodaTextSocketReceiver.getRecId(outputOperationCompleted.outputOperationInfo.id))
        //logger.info(s"description ${outputOperationCompleted.outputOperationInfo.description}")
        logger.info(s"id ${outputOperationCompleted.outputOperationInfo.id}")
        logger.info(s"name ${outputOperationCompleted.outputOperationInfo.name}")
        logger.info(s"startTime ${outputOperationCompleted.outputOperationInfo.startTime}")
        logger.info(s"endTime ${outputOperationCompleted.outputOperationInfo.endTime}")
        logger.info(s"failure reason (if any) ${outputOperationCompleted.outputOperationInfo.failureReason}")
        IKodaTextSocketReceiver.setActiveReceiving(outputOperationCompleted.outputOperationInfo.id,false)
        ProcessStatus.incrementStatus("Stream: Output Completed")
        if(outputOperationCompleted.outputOperationInfo.failureReason.isDefined)
          {
            ProcessStatus.incrementStatus("Stream: Output Failures")
          }

        statusMap.put(outputOperationCompleted.outputOperationInfo.id, false)
      }
    }
  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit =
  {
    val bo=statusMap.get(outputOperationStarted.outputOperationInfo.id)
    if(bo.isDefined)
    {
      if (true==bo.get)
      {
        logger.info(IKodaTextSocketReceiver.getRecId(outputOperationStarted.outputOperationInfo.id))
        logger.info("  Listener says outputOperationStarted\n")

        logger.info(s"id ${outputOperationStarted.outputOperationInfo.id}")
        logger.info(s"name ${outputOperationStarted.outputOperationInfo.name}")
        logger.info(s"startTime ${outputOperationStarted.outputOperationInfo.startTime}")
        logger.info(s"endTime ${outputOperationStarted.outputOperationInfo.endTime}")
        logger.info(s"failure reason (if any) ${outputOperationStarted.outputOperationInfo.failureReason}")
      }
    }
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit =
  {
    if (IKodaTextSocketReceiver.isEnabled(receiverError.receiverInfo.streamId))
    {
      logger.info("  Listener says receiverError\n")
      logger.info(IKodaTextSocketReceiver.getRecId(receiverError.receiverInfo.streamId))
      logger.info(s"streamId ${receiverError.receiverInfo.streamId}")
      logger.info(s"name ${receiverError.receiverInfo.name}")
      logger.info(s"active ${receiverError.receiverInfo.active}")
      logger.info(s"executorId ${receiverError.receiverInfo.executorId}")
      logger.info(s"location ${receiverError.receiverInfo.location}")
    }
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit =
  {
    if (IKodaTextSocketReceiver.isEnabled(receiverStopped.receiverInfo.streamId))
    {
      /**  logger.info("----Listener says receiverStopped----")
      logger.info(IKodaTextSocketReceiver.getRecId(receiverStopped.receiverInfo.streamId))
      logger.info(s"streamId ${receiverStopped.receiverInfo.streamId}")
      logger.info(s"name ${receiverStopped.receiverInfo.name}")
      logger.info(s"active ${receiverStopped.receiverInfo.active}")
      logger.info(s"executorId ${receiverStopped.receiverInfo.executorId}")
      logger.info(s"location ${receiverStopped.receiverInfo.location}")**/
    }
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit =
  {
    //logger.info("  Listener says streamingStarted\n")
    //logger.info(s"time ${streamingStarted.time}")

  }
}
