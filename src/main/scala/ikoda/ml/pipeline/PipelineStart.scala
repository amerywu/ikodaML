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

    val analysisType2:Try[String] = Try(ConfigFactory.load("scalaML").getString("scalaML.launchRoutine.task2"))
    analysisType2.isSuccess match
      {
      case true =>
        val pconfig1 = CustomPipelineConfiguration(analysisType2.get)
        if(pconfig.get(PipelineConfiguration.analysisType) == CustomPipelineConfiguration.redditSentenceLevel ||
           pconfig.get(PipelineConfiguration.analysisType) == CustomPipelineConfiguration.redditDocumentLevel ||
           pconfig.get(PipelineConfiguration.analysisType) == CustomPipelineConfiguration.jobsDocumentLevel ||
           pconfig.get(PipelineConfiguration.analysisType) == CustomPipelineConfiguration.jobsSentenceLevel
        )
          {
            pconfig1.passValue(PipelineConfiguration.pipelineOutputRoot,pconfig.get(PipelineConfiguration.pipelineOutputRoot))
            pconfig1.passValue(PipelineConfiguration.phraseAnalysisDataSourceRootPath,pconfig.get(PipelineConfiguration.pipelineOutputRoot))
            pconfig1.passValue(PipelineConfiguration.phraseAnalysisReportPath,pconfig.get(PipelineConfiguration.pipelineOutputRoot))


          }
        logger.info("\n\n\n=============================\nNext task is "+analysisType2.get)
        pconfig1.runPipeline(PipelineConfiguration._mainMethodMap)
      case false => logger.info("Nothing left to do")
    }

    logger.info("\n\n\tPIPELINE COMPLETED SUCCESSFULLY\n\n")


  }
  
  

  

  
  

  
  
  

  
  
}
