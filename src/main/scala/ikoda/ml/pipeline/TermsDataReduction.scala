package ikoda.ml.pipeline

import java.io.File

import better.files._
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.sparse.{CellTuple, RDDLabeledPoint}
import ikoda.utilobjects.{DataFrameUtils, SimpleLog, SparkConfProviderWithStreaming, UtilFunctions}
import ikoda.utils.{Spreadsheet, TicToc}
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}




/**
  * Utility methods for reducing data
  */
class TermsDataReduction extends Logging with SimpleLog  with SparkConfProviderWithStreaming with UtilFunctions
{



  
  private def tempDir(outputPath:String): String =
  {
    val tempPath:String=outputPath+File.separator+"tmp"
    val dir: better.files.File = tempPath.toFile.createIfNotExists(true, true)
    tempPath
  }
  
  @throws(classOf[IKodaMLException])
  private def loadSparseList(filesInPath:List[String], fileNameCore:Seq[String]): ListBuffer[RDDLabeledPoint] =
  {
    try
    {
      val sparseList:ListBuffer[RDDLabeledPoint] = new ListBuffer()
      filesInPath.foreach
      {
        filePath =>
      
          fileNameCore.foreach
          {
            fcore =>
              if(filePath.contains(fcore)&&filePath.toLowerCase().contains(".libsvm"))
              {
                logger.debug(s"loading $filePath")
                addLine(s"Loading: $filePath")
                val sparse0: RDDLabeledPoint = new RDDLabeledPoint
                sparse0.sparseData().repartition(2000)

                sparseList += getOrThrow(RDDLabeledPoint.loadLibSvmLocal( filePath))
              }
          }
      }
      sparseList
    }
    catch
      {
        case e:Exception => throw new IKodaMLException(e.getMessage,e)
      }
  }

  @throws(classOf[IKodaMLException])
  private def  mergeLibSvms(pconfig: PipelineConfiguration,reducedList:List[RDDLabeledPoint]): RDDLabeledPoint =
  {
    try
    {
      logger.info("Merging subsets.")
      Spreadsheet.getInstance().initLibsvm2(
        "_f",
        pconfig.get(PipelineConfiguration.targetColumn),
        tempDir(pconfig.get(PipelineConfiguration.pipelineOutputRoot)))

      Spreadsheet.getInstance().getLibSvmProcessor("_f").setPkColumnName(pconfig.get(PipelineConfiguration.uidCol))
      Spreadsheet.getInstance().getLibSvmProcessor("_f").loadLibsvm("_part0")
  
      reducedList.tail.foreach
      {
        var count=1;
        spasepart=>
          Spreadsheet.getInstance().initLibsvm2(s"_part${count}",
            pconfig.get(PipelineConfiguration.targetColumn),
            tempDir(pconfig.get(PipelineConfiguration.pipelineOutputRoot)))

          Spreadsheet.getInstance().getLibSvmProcessor(s"_part${count}").setPkColumnName(pconfig.get(PipelineConfiguration.uidCol))

          Spreadsheet.getInstance().getLibSvmProcessor(s"_part${count}").loadLibsvm(s"_part${count}")
          logger.info(s"Merging _part${count}")
          val m=Spreadsheet.getInstance().getLibSvmProcessor("_f").mergeIntoLibsvm(Spreadsheet.getInstance().getLibSvmProcessor(s"_part${count}"))
          addLine(m)
          Spreadsheet.getInstance().getLibSvmProcessor(s"_part${count}").clearData()
          count=count+1
      }
  
      logger.info("saving pj reduced and merged")
      val ignore:Array[String] = Seq[String]("A_RowId").toArray
      Spreadsheet.getInstance().getLibSvmProcessor("_f").printLibSvmFinal(
        s"${pconfig.get(PipelineConfiguration.prefixForMerging)}_I"
        , pconfig.get(PipelineConfiguration.targetColumn)
        , ignore)
  
      Spreadsheet.getInstance().getLibSvmProcessor("_f").clearData()
      val  sparse1:RDDLabeledPoint=new RDDLabeledPoint

      getOrThrow(RDDLabeledPoint.loadLibSvmLocal(s"${tempDir(pconfig.get(PipelineConfiguration.pipelineOutputRoot))}${File.separator}FINAL_${pconfig.get(PipelineConfiguration.prefixForMerging)}_I"))

    }
    catch
      {
        case e:Exception => throw new IKodaMLException(e.getMessage,e)
      }
  }
  
  
  def setSimpleLog(path:String, fileName:String): Unit =
  {
    initSimpleLog(path,fileName)
  }



  
  



  def sumNonDuplicates(duplicateCounts:Seq[(Double,Int,Int)]): Seq[(Double,Int,Int, Double)] =
  {
    //val nonDuplicate:Int=duplicateCounts.filter(e=>e._3 ==1).size

    val total=duplicateCounts.map(e=>e._3).sum
    //logger.debug("Total="+total)
    duplicateCounts.filter(e=>e._3 >1).map
    {
      de=> (de._1,de._2,de._3,de._3.toDouble/total.toDouble)
    }
  }



  @throws
  def analyzeDuplicates(sparse0:RDDLabeledPoint): Seq[(Double,Int,Int,Double)] =
  {
    try
    {

      val returnList:ArrayBuffer[(Double,Int,Int,Double)]=new ArrayBuffer
      sparse0.getTargets().foreach {
        tgt =>
          val sparseFortarget = sparse0.sparseData().filter(e=>e._1.label==tgt._2)
            val groupedCountByTarget = sparseFortarget.map {
              r => (r._2, 1)
            }.reduceByKey((a, b) => a + b).map{
             e=> (tgt._2,e._1,e._2)
           }.collect().filter(e=> e._2 > 0 )
            returnList ++= sumNonDuplicates(groupedCountByTarget)
      }
      returnList
    }
    catch
      {
        case e:Exception =>
        {
          logger.error(e.getMessage,e)
          throw new Exception(e)
        }
      }
  }
  
 
  
  
  @throws
  def dataCompileRowBlocks(path:String, targetColumnName:String, uidColName:String, prefix:String,columnsToIgnore:List[String],fileNames:String*): Unit =
  {
    try
    {
      val tt = new TicToc()
      logger.info(tt.tic("Merging Files "))
      val dfList:List[String] = fileNames.toList
      logger.info(s"${dfList.mkString(",")}")

      dfList.foreach
      {
        filename =>
          logger.info(tt.tic(s"Processing $filename"))
          Spreadsheet.getInstance().initCsvSpreadsheet(filename,path)
          Spreadsheet.getInstance().getCsvSpreadSheet(filename).setTargetColumnName(targetColumnName)
          Spreadsheet.getInstance().getCsvSpreadSheet(filename).setPkColumnName(uidColName)
          Spreadsheet.getInstance().getCsvSpreadSheet(filename).finalizeAndJoinBlocksLibSvm(filename,columnsToIgnore.toArray)
          logger.info(tt.toc())
      }
      logger.info(tt.toc("Merging Files"))

    }
    catch
    {
      case e:Exception =>
      {
        logger.error(e.getMessage,e)
        throw new Exception(e)
      }
    }
  }
  
  
  
  
  
  



  @throws(classOf[IKodaMLException])
  def getValueAtPercentile(columns:mutable.HashMap[Int, CellTuple], percentile:Double): Double =
  {
    getValuePercentile(columns.toMap,percentile)
  }
  
  @throws(classOf[IKodaMLException])
  def getValuePercentile(columns:Map[Int, CellTuple], percentile:Double): Double =
  {
    try
    {
      val stats = new DescriptiveStatistics

      val listOfDoubles:List[Double]=columns.values.toList.sorted.map
      {
        ct => ct.value
      }.toList.sorted

      getValueAtPercentile(listOfDoubles,percentile)
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage,e)
    }
  }
  
  @throws(classOf[IKodaMLException])
  def getValueAtPercentile(listOfDoubles:List[Double], percentile:Double): Double =
  {
    try
    {
      val stats = new DescriptiveStatistics


      listOfDoubles.sorted.foreach
      {
  

        v =>
          
          stats.addValue(v)
        
      }
      stats.getPercentile(percentile)
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage,e)
    }
  }
  
  
  

  
  
  
  
  @throws (classOf[IKodaMLException])
  private def removeLowProportionFromSparse1(sparse0:RDDLabeledPoint, lowerThreshold:Double):RDDLabeledPoint=
  {
    try
    {
      val tt:TicToc=new TicToc()

      val results:Seq[(Int, Double)]=RDDLabeledPoint.proportionOfColumnWithValues(sparse0)

      val filteredResults:Seq[(Int, Double)]=results.filter(t=> t._2<0.005)

      
 
      logger.info(s"from sparse ${sparse0.columnCount}")
      
 
      val toRemove=filteredResults.map
      {
        t=> t._1
      }.toSet
      
      val sparseOut=getOrThrow(RDDLabeledPoint.removeColumnsDistributed(sparse0, toRemove))
     
      
      
      
      
      logger.info(tt.toc("Removing low proportion columns from sparse"))
      sparseOut
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
  
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  
  @throws (classOf[IKodaMLException])
  def removeHighProportionFromSparse( sparse0:RDDLabeledPoint,pconfig: PipelineConfiguration):Tuple2[RDDLabeledPoint, mutable.HashMap[String,String]]=
  {
    try
    {
      val tt:TicToc=new TicToc()
      logger.info(tt.tic("Removing high proportion columns from sparse"))
      val results:Seq[(Int, Double)]=RDDLabeledPoint.proportionOfColumnWithValues(sparse0)
      
      val threshold:Double=getValueAtPercentile(results.map(t=> t._2).toList,pconfig.getAsDouble(PipelineConfiguration.cr_highFrequencyPercentileToRemove))

      addLine("For each column, calculating  (number of cells where value greater than zero)/(total number of rows)")
      addLine(s"Removing columns where proportion of cells with values > $threshold. If ${threshold} number of cells have a value greater than 0, remove the column. ")
      logger.info(s"Removing high freq columns where count non zero  > $threshold")
      val filteredResults:Seq[(Int, Double)]=results.filter(t=> t._2>threshold)
      

      val highFreqMap:mutable.HashMap[String,String]=new mutable.HashMap[String,String]()
      val sb:mutable.StringBuilder=new mutable.StringBuilder()
      filteredResults.foreach
      {
        c => highFreqMap.put(sparse0.getColumnName(c._1),s"${c._2}")
          sb.append(sparse0.getColumnName(c._1))
          sb.append(",")
      }

      logger.debug(s"from sparse ${sparse0.columnCount}")
      pconfig.config(PipelineConfiguration.cr_stopwords,sb.toString())
      
      
      val toRemove=filteredResults.map
      {
        t=> t._1
      }.toSet
      
      val sparseOut=getOrThrow(RDDLabeledPoint.removeColumnsDistributed( sparse0,toRemove))

      logger.info(tt.toc)
      (sparseOut,highFreqMap)
      
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        
        throw new IKodaMLException(e.getMessage, e)
    }
  }
  
  @throws (classOf[IKodaMLException])
  def removeLowFreqFromSparse( sparse0:RDDLabeledPoint, pconfig:PipelineConfiguration):RDDLabeledPoint=
  {
    try
    {
      val tt:TicToc=new TicToc
      logger.info(tt.tic("Removing low freq columns from sparse"))
      val colSums:Map[Int, CellTuple] = RDDLabeledPoint.colSums(sparse0)

      val valueList: List[Double] = colSums.map(ct => ct._2.value).toList.sorted
      val median = valueList(valueList.length / 2)
      val offset=pconfig.getAsInt(PipelineConfiguration.cr_medianOffsetForLowFrequency)
      val toDropFromSparse = colSums.values.filter(ct => ct.value < (median+offset)).map(ct => ct.colIndex).toSeq
      

      addLine(s"median is $median")
      addLine(s"adjusted criteria is ${median+offset}")
      addLine(s"current column count ${sparse0.columnCount}")
      addLine(s"removing ${toDropFromSparse.length} columns ")
      val sparseOut=getOrThrow(RDDLabeledPoint.removeColumnsDistributed(sparse0, toDropFromSparse.toSet))
      addLine(s"New column count ${sparseOut.info}")
      logger.info(tt.toc)
      sparseOut
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        throw new IKodaMLException(e.getMessage,e)
    }
  }


  private def removeNthPercentileFromSparse( sparse0:Option[RDDLabeledPoint], percentile:Int=50):Option[RDDLabeledPoint]=
  {
    sparse0.isDefined match
      {
      case true => removeNthPercentileFromSparse(sparse0.get,percentile)
      case _ => None
    }
  }
  
  @throws (classOf[IKodaMLException])
  private def removeNthPercentileFromSparse( sparse0:RDDLabeledPoint, percentile:Int):Option[RDDLabeledPoint]=
  {
    try
    {
      val tt:TicToc=new TicToc
      logger.info(tt.tic("Removing nth percentile freq  from sparse"))
      val colSums:Map[Int, CellTuple] = RDDLabeledPoint.colSums(sparse0)

      val percentile80=getValuePercentile(colSums,80)
      addLine(s"Frequency count at 80th percentile is $percentile80")
      val percentile20=getValuePercentile(colSums,20)
      addLine(s"Frequency count at 20th percentile is $percentile20")

      val percentileValue=getValuePercentile(colSums,percentile)

      val valueList: List[Double] = colSums.map(ct => ct._2.value).toList.sorted

      val toDropFromSparse = colSums.values.filter(ct => ct.value < percentile).map(ct => ct.colIndex).toSeq


      addLine(s"Making cut at percentile $percentile which is a frequency count of $percentileValue\n")
      
      logger.info(s"current column count ${sparse0.columnCount}")
      logger.info(s"removing ${toDropFromSparse.length} columns ")
      val sparseOut=getOrThrow(RDDLabeledPoint.removeColumnsDistributed(sparse0, toDropFromSparse.toSet))
      logger.info(s"New column count ${sparseOut.columnCount}")
      

      logger.info(tt.toc)
      Some(sparseOut)
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        throw new IKodaMLException(e.getMessage,e)
    }
  }
  
  
  
  @throws(classOf[IKodaMLException])
  private def reduceByCategory(sparse0:RDDLabeledPoint):RDDLabeledPoint=
  {
     try
    {
  

      logger.info("\n\n\nREDUCE\n\n")

      getOrThrow(RDDLabeledPoint.reduceToTargetBySum1(sparse0))

    }
    catch
      {
  
        case e:Exception=> throw new IKodaMLException(e.getMessage(),e)
      }
    
      
  }
  
  private def normalize(sparse:RDDLabeledPoint): RDDLabeledPoint =
  {
    try
    {
    
      logger.info("\n\n\nROW SUMS\n\n")
      val rowSumsMap0: HashMap[Double, CellTuple] = RDDLabeledPoint.rowSums(sparse)

      logger.info("\n\n\nROW SUMS OPERATION\n\n")
      val sparseNormalizeStep1=getOrThrow(RDDLabeledPoint.rowCellOnRowConstant1( sparse,sparse._dividedBy, rowSumsMap0))
      logger.debug(s"Biggish data after row sums row count ${sparseNormalizeStep1.rowCountEstimate}")

      logger.info("\n\n\nCOL SUMS\n\n")
      val colSumsMap0: Map[Int, CellTuple] = RDDLabeledPoint.colSums(sparseNormalizeStep1)
      logger.debug(colSumsMap0.keySet.toSeq.sorted.toString())

      logger.info("\n\n\nCOL SUMS OPERATION\n\n")
      getOrThrow(RDDLabeledPoint.columnCellOnColumnConstant( sparseNormalizeStep1,sparseNormalizeStep1._dividedBy, colSumsMap0))

    }
    catch
    {
      case e:Exception => throw new IKodaMLException(e.getMessage,e)
    }

  }

   def normalizeRowWise(sparse0:RDDLabeledPoint): RDDLabeledPoint =
  {
    try
    {

      logger.info("\n\n\nROW SUMS\n\n")
      val rowSumsMap0: HashMap[Double, CellTuple] = RDDLabeledPoint.rowSums(sparse0)

      logger.info("\n\n\nROW SUMS OPERATION\n\n")
      getOrThrow(RDDLabeledPoint.rowCellOnRowConstant1( sparse0,sparse0._dividedBy, rowSumsMap0))

      

    }
    catch
      {
        case e:Exception => throw new IKodaMLException(e.getMessage,e)
      }
    
  }

  def reduceByTfIdf(sparse0:Option[RDDLabeledPoint], percentileToCut:Int): Option[RDDLabeledPoint] =
  {
    val sparseTf=RDDLabeledPoint.termFrequencyNormalization(sparse0)
    val sparseTfIdf=RDDLabeledPoint.inverseDocumentFrequency(sparseTf)

    removeNthPercentileFromSparse(sparseTfIdf,percentileToCut)
  }
  

  

  
  def stDevForEachColumn(sparse0:RDDLabeledPoint): Map[Int,CellTuple] =
  {
    val stDevsMap:Map[Int,CellTuple]=RDDLabeledPoint.colStDevs(sparse0)

    val percentile80=getValuePercentile(stDevsMap,80)
    logger.info(s"80th percentile is $percentile80")
    val percentile20=getValuePercentile(stDevsMap,20)
    logger.info(s"20th percentile is $percentile20")
    stDevsMap
  }


  @throws(classOf[IKodaMLException])
  def normalizeAndProportionalize(sparse0:RDDLabeledPoint,outputPath:String ): RDDLabeledPoint =
  {
    try
    {
      val sparseReduced:RDDLabeledPoint= reduceByCategory(sparse0)
      require(RDDLabeledPoint.validateSparseDataSchemaMatch(sparseReduced,sparse0))
      normalize(sparseReduced)
    }
    catch
      {
        case ex: IKodaMLException =>
        {
          logger.error(ex.getMessage, ex)
          throw new IKodaMLException(ex.getMessage,ex)
        }
      }

  }


 
  
  
}
