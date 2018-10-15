package ikoda.ml.predictions

import ikoda.utils.StreamingConstants

trait MLServiceUtils {


  val dataTagSeq:Seq[String]= Seq(StreamingConstants.IKODA_DATA_NAME,StreamingConstants.IKODA_DATA_FORMAT,StreamingConstants.IKODA_END_STREAM)



  def containsTag(s:String): Boolean =
  {
    dataTagSeq.map(t=>s.contains(t)).contains(true)
  }

}
