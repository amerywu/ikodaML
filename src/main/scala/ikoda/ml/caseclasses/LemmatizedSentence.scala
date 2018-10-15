package ikoda.ml.caseclasses



case class LemmatizedSentence(
                               sparseRow:Tuple3[org.apache.spark.ml.feature.LabeledPoint,Int,String],
                               rawSentence:String,
                               lemmatizedSentence:String,
                               clusterId:String,
                               target:Double,
                               targetName:String,
                               term1:String,
                               term1Value:Double,
                               term2:String,
                               term2Value:Double,
                               subset:String)
{



}
