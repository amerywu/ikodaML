package ikoda.ml

import grizzled.slf4j.Logging
import ikoda.ml.pipeline.PipelineMethods
import ikoda.utilobjects.{ SparkConfProviderWithStreaming}
import org.junit.Test

class PipelineTest extends Logging with SparkConfProviderWithStreaming with PipelineMethods
{

  @Test
  def testSpark(): Unit =
  {
    //mergeLabels
  }


}
