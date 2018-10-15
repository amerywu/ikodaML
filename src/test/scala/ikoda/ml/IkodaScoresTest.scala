package ikoda.ml

import grizzled.slf4j.Logging
import ikoda.ml.pipeline.IKodaScores
import ikoda.utilobjects.{ SparkConfProviderWithStreaming}
import ikoda.utils.Spreadsheet

//import
import org.junit.Assert.{assertTrue, fail}
import org.junit.Test


class IkodaScoresTest extends Logging with SparkConfProviderWithStreaming
{
  @Test
  def testSpark(): Unit =
  {
    try
    {
      val is=new IKodaScores()

      logger.info("------------------\nCompiling\n------------------")
     //is.dataCompileRowBlocks("C:\\Users\\jake\\__workspace\\scalaProjects\\scalaML\\input","A_RowId",List[String](),"Report_")
      logger.info("------------------\nShifting Columns\n------------------")
    // is.renameCols("FINAL_ikMajors.csv","C:\\Users\\jake\\__workspace\\scalaProjects\\scalaML\\input")
      logger.info("------------------\nGathering Score Data\n------------------")
      //is.processiKodaScores("./input/FINAL_cleanedData.csv")
      logger.info("------------------\nDone\n------------------")

      assertTrue(true)
    }
    catch
    {
      case ex: Exception =>
      {
        
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}",ex)
        fail(s"${ex.getMessage} ${ex.printStackTrace()}")
        //fail()
      }
    }
  }
  
  @throws
  private def shiftCols(fileName:String, dir:String): Unit =
  {
    try
    {
    
      Spreadsheet.getInstance().initCsvSpreadsheet("colShift","ikoda.ml",dir)
      Spreadsheet.getInstance().getCsvSpreadSheet("colShift").loadCsv(fileName,"A_RowId");
      val colnames:Array[String]=Spreadsheet.getInstance().getCsvSpreadSheet("colShift").getColumnNames
      
      
      val colSeq=colnames.toSeq
      //logger.debug(colSeq.mkString("\n"))
      
      colnames.toSeq.foreach
      {
        col =>
          
          if(col.startsWith("1637_"))
            {
              val colTruncated=col.substring(5,col.length)
              logger.debug(colTruncated)
              val newColName:String="1279838_"+colTruncated
              logger.debug(newColName)
              Spreadsheet.getInstance().getCsvSpreadSheet("colShift").renameColumn(col,newColName)
            }
      }

      Spreadsheet.getInstance().getCsvSpreadSheet("colShift").printCsvFinal("testOut")

      logger.debug(Spreadsheet.getInstance().getCsvSpreadSheet("colShift").getColumnNames.toSeq.mkString("\n--"))
      
    }
    catch
    {
      case ex: Exception =>
      {
      
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}",ex)
        throw new Exception(ex);
        //fail()
      }
    }
  }
}
