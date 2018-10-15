package ikoda.ml.pipeline

import better.files._
import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import ikoda.utilobjects.{SimpleLog, SparkConfProviderWithStreaming}

import scala.util.Try

object PipelineStart extends Logging with SimpleLog  with SparkConfProviderWithStreaming
{


  def main(args: Array[String]): Unit = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    println(currentDirectory)
    logger.info("Logger On ")


    val analysisType = ConfigFactory.load("scalaML").getString("scalaML.launchRoutine.task1")
    logger.info(s"\n\n\t$analysisType\n\n")
    logger.info("Analysis Type: " + analysisType)
    val pconfig = CustomPipelineConfiguration(analysisType)
    val dir: better.files.File = pconfig.get(PipelineConfiguration.pipelineOutputRoot).toFile.createIfNotExists(true, true)
    logger.info("Logger On 3")
    pconfig.runPipeline(PipelineConfiguration._mainMethodMap)



    logger.info("\n\n\tPIPELINE COMPLETED SUCCESSFULLY\n\n")


  }
  
  

  

  
  

  
  
  

  
  
}
