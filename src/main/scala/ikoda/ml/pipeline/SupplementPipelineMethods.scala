package ikoda.ml.pipeline

import java.io.File

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.cassandra.{SparseDataToRDDLabeledPoint, SparseSupplementRetriever}
import ikoda.utilobjects._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

trait SupplementPipelineMethods extends Logging with SimpleLog with SparkConfProviderWithStreaming with UtilFunctions with StringPredicates with IntPredicates with PipelineFunctionTypes{

  val printSupplementByLabel: FTDataReductionProcessDataFrame= (pconfig:PipelineConfiguration) => (odf:Option[DataFrame]) =>{
    try {
      if(odf.isDefined){

        val targetLoader:SparseDataToRDDLabeledPoint=new SparseDataToRDDLabeledPoint(pconfig)
        val targetsMap=targetLoader.loadTargets().filter(e=> !e._1.contains("#"))
        val sqlContext=spark.sqlContext

        import sqlContext.implicits._

        targetsMap.foreach
        {
          t=>
            val target=t._1
            logger.info("Saving target from target map: "+target)
            val dfByLabel:DataFrame=odf.get.filter($"aa_label".startsWith(target))
            val fileName=s"file:///${pconfig.get(PipelineConfiguration.pathToSpark)}${File.separator}${pconfig.get(PipelineConfiguration.pipelineOutputRoot)}${File.separator}Supplement_${t._1}.csv"
            logger.info("Saving to "+fileName)
            logger.info("dfByLabel count: "+dfByLabel.count())
            dfByLabel.coalesce(1).write.csv(fileName)
            addLine("Printed "+fileName)
        }
      }
      odf
    }
    catch {
      case e: Exception => throw new IKodaMLException("loadSparseSupplement: " + e.getLocalizedMessage, e)
        None
    }
  }

}
