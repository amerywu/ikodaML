package ikoda.ml

import grizzled.slf4j.Logging
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.{ SparkConfProviderWithStreaming}
//import ikoda.nlp.analysis.CollegeURLAnalyzerThread
import org.junit.Assert._
import org.junit._


@Test
class CassandraTest extends Logging with SparkConfProviderWithStreaming
{
  
  
  val sparseTrainingSet: RDDLabeledPoint = new RDDLabeledPoint
  
  
  @Test
  def testCassandra(): Unit =
  {
    try
    {
      logger.info("testCassandra Test")

      
      assertTrue(true)
    }
    catch
    {
      case ex: Exception =>
      {
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}")
      }
    }
  }

  
}


