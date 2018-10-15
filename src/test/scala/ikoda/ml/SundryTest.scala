package ikoda.ml



import grizzled.slf4j.Logging
import ikoda.utilobjects.{SparkConfProviderWithStreaming}
import org.junit._

@Test
class SundryTest extends Logging with SparkConfProviderWithStreaming
{
  type FToperationType = (Double, Double) => Double
  
  def applyTransformations(initial: String, transformations: Seq[String => String]) =
    transformations.foldLeft(initial) {
      (cur, transformation) => transformation(cur)
    }
  
  
  val divide:FToperationType=
  {
    _/_
  }
  
  def doOperation(d1:Double, operation:FToperationType, d2:Double): Double =
  {
    operation(d1,d2)
  }
  
  
  @Test
  def testSundry(): Unit =
  {
    

    try
    {

    
    }
    catch
    {
      case ex: Exception =>
      {
        
        println(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}")
      }
    }
  }
  
  
  
}


