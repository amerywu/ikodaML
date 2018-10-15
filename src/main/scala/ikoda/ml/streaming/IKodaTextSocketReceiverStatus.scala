package ikoda.ml.streaming

case class IKodaTextSocketReceiverStatus(
                                          receiverId: String, enabled: Boolean, activeReceiving: Boolean,
                                          dataName: String,  streamId: Int, keySpaceName:Option[String], keySpaceUUID:Option[String], kill:Boolean
                                        )
{

}
