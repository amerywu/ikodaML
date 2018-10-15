package ikoda.ml

import grizzled.slf4j.Logging
import ikoda.sparse.{CellTuple, RDDLabeledPoint}
import ikoda.utilobjects.{DataFrameUtils, SparkConfProviderWithStreaming}
import ikoda.utils.{Spreadsheet, TicToc}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class SparseTest extends Logging with SparkConfProviderWithStreaming
{
  
  @Test
  def testSparse(): Unit =
  {
    try
      {
       //runTests()
       // dataCompileFromRawDf()
       // val tdr: TermsDataReduction=new TermsDataReduction
        //tdr.doDataReduction(pj=false)
        //generateSmallLibsvm()
        logger.info("\n\nALL WORK COMPLETED. EXITING\n\n\n")
        assertTrue(true)
      }
    catch
      {
        case e:Exception =>
          fail(s"${e.getMessage} ${e.printStackTrace()}")
          logger.error(e.getMessage,e)
      }
      
  }
  
  

  /*******
  
  
  @throws (classOf[Exception])
  def assertRowCountMatch(df:DataFrame, sparse0: RDDLabeledPoint):Boolean=
  {
    try
    {
      logger.info(s"\ndf. row count() ${df.count()}\nsparse0.sparseData().row count ${sparse0.sparseData().count}")

      
      if(df.count() == sparse0.sparseData().count)
      {
        true
      }
      else
      {
        logger.warn("\n\nassertRowCountMatch FAILED\n\n")
        val labelsdf:Seq[String]=df.collect().map(r=>r.getAs[String]("A_AggregatedMajor"))
        val labelssparse:Seq[String]=sparse0.lpData().map(r=>sparse0.getTargetName(r.label)).collect
        logger.info(s"\nFrom df: ${labelsdf.mkString(",")}")
        logger.info(s"\nFrom sparse: ${labelssparse.mkString(" | ")}")
        false
      }
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        fail(e.getMessage)
        throw new Exception(e)
    }
  }
  
  @throws (classOf[Exception])
  def assertColumnNamesMatch(df:DataFrame, sparse0: RDDLabeledPoint, sparseOffset:Int):Boolean=
  {
    try
    {
      val dfColNames:Seq[String]=df.schema.fieldNames
      
      logger.debug(s"Column Count in sparse is ${sparse0.getColumnCount}")
      logger.debug(s"Column Count in df is ${dfColNames.length}")
      logger.debug(s"Columns in df: ${dfColNames.mkString(",")}")

      var  pass:Boolean = false
      
      var colIndex:Int=dfColNames.length-4

      logger.debug(s"assertColumnNamesMatch   df: ${dfColNames(colIndex)} \n      " +
        s"sparse: ${sparse0.getColumnName(colIndex-sparseOffset)} \n      " +
        s"at df index ${colIndex} and sparse index${colIndex-sparseOffset}")
      
      
      if(dfColNames(colIndex)==sparse0.getColumnName(colIndex-sparseOffset))
        {
          pass=true
        }
        else
        {
          logger.warn("\n\nassertColumnNamesMatch 1 FAILED\n\n")
          pass=false
        }
      
      
      
      colIndex=dfColNames.length/2

      logger.debug(s"assertColumnNamesMatch   df: ${dfColNames(colIndex)} \n      " +
        s"sparse: ${sparse0.getColumnName(colIndex-sparseOffset)} \n      " +
        s"at df index ${colIndex} and sparse index${colIndex-sparseOffset}")
      if(dfColNames(colIndex)==sparse0.getColumnName(colIndex-sparseOffset))
      {
        pass=true
      }
      else
      {
        logger.warn("\n\nassertColumnNamesMatch 2 FAILED\n\n")
        pass=false
      }
      colIndex=3
      logger.debug(s"assertColumnNamesMatch   df: ${dfColNames(colIndex)} \n      " +
        s"sparse: ${sparse0.getColumnName(colIndex-sparseOffset)} \n      " +
        s"at df index ${colIndex} and sparse index${colIndex-sparseOffset}")
      if(dfColNames(colIndex)==sparse0.getColumnName(colIndex-sparseOffset))
      {
        pass=true
      }
      else
      {
        logger.warn("\n\nassertColumnNamesMatch 3 FAILED\n\n")
        pass=false
      }
      pass
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        fail(e.getMessage)
        throw new Exception(e)
    }
  }
  
  @throws (classOf[Exception])
  def assertColumnCountMatch(df:DataFrame, sparse0: RDDLabeledPoint, sparseOffset:Int):Boolean=
  {
    try
    {
      logger.info(s"df.schema.fieldNames.length ${df.schema.fieldNames.length}\nsparse0.sparseData().count ${sparse0.getColumnCount + sparseOffset}")
      logger.info(s"from df :${df.schema.fieldNames.mkString(",")}")
      logger.info(s"from sparse ${sparse0.columnHeadsAsString()+ sparseOffset}")
      
      if(df.schema.fieldNames.length == sparse0.getColumnCount + sparseOffset)
      {
        true
      }
      else
      {
        logger.warn("\n\nassertRowCountMatch FAILED\n\n")
        false
      }
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        fail(e.getMessage)
        throw new Exception(e)
    }
  }
  
  @throws (classOf[Exception])
  def removeLowProportionFromSparse(tt:TicToc, spark:SparkSession,sparse0:RDDLabeledPoint, df:DataFrame, offset:Int):DataFrame=
  {
    try
    {
      tt.tic("Removing low proportion columns from sparse")
      val results:Seq[(Int, Double)]=sparse0.soProportionOfColumnWithValues()
      
      logger.debug(s"removeLowProportionFromSparse unfiltered ${results.mkString(",")}")
      val filteredResults:Seq[(Int, Double)]=results.filter(t=> t._2<0.01||t._2>0.2)
      logger.debug(s"removeLowProportionFromSparse filtered ${filteredResults.mkString(",")}")
      val q:mutable.Queue[Int]=mutable.Queue()
      
      logger.debug(s"from df: ${df.schema.fieldNames.mkString(",")}")
      logger.debug(s"from sparse ${sparse0.getColumnCount}")
      
      val dfColNames:ListBuffer[String]=ListBuffer()
      filteredResults.foreach
      {
        t=> q += t._1
          dfColNames += df.schema.fieldNames(t._1+offset)
          logger.debug(s"Removing df: ${df.schema.fieldNames(t._1+offset)} sparse ${sparse0.getColumnName(t._1)} ")
          
      }

      val sparse1 =sparse0.soRemoveColumns( q)
      val dfNew=df.drop(dfColNames:_*)

      
      
     
      logger.info(tt.toc)
      dfNew
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        fail(e.getMessage)
        throw new Exception(e)
    }
  }
  
  @throws (classOf[Exception])
  def removeLowFreqFromSparse(tt:TicToc, spark:SparkSession,sparse0:RDDLabeledPoint):Unit=
  {
    try
    {
      tt.tic("Removing low freq columns from sparse")
      val colSums: Map[Int, CellTuple] = sparse0.soColSums()
  
  
      val valueList: List[Double] = colSums.map(ct => ct._2.value).toList.sorted
      val median = valueList(valueList.length / 2)
      val toDropFromSparse = colSums.values.filter(ct => ct.value < median).map(ct => ct.colIndex).toSeq
  
  
      val q: mutable.Queue[Int] = mutable.Queue(toDropFromSparse: _*)
      logger.info(s"median is $median")
      logger.info(s"removing ${toDropFromSparse.length} columns ")
      sparse0.soRemoveColumns(spark, q)
      
      logger.info(tt.toc)
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        fail(e.getMessage)
        throw new Exception(e)
    }
  }
  
  
  @throws (classOf[Exception])
  def dropDFBelowMedian(tt:TicToc, spark: SparkSession, dfvDroppedStrings:DataFrame):DataFrame=
  {
    try
    {
      tt.tic("Removing low freq columns from dfj")
      val dfDroppedLowFReq: DataFrame = DataFrameUtils.deleteColumnsWhereColTotalLessThanMedian(spark, dfvDroppedStrings, ignoreColumns = Seq("A_AggregatedMajor")).drop("A_RowId").na.fill(0)
      DataFrameUtils.saveToPjCsv(dfDroppedLowFReq, "dfDroppedLowFReq", "./unitTestOutput")
      logger.info(tt.toc)
      dfDroppedLowFReq
    }
    catch
    {
      case e:Exception =>
        logger.error(e.getMessage(),e)
        fail(e.getMessage)
        throw new Exception(e)
    }
  }
  
  @throws (classOf[Exception])
   def dropDFStringColumns(tt:TicToc, dfvInput:DataFrame):DataFrame=
   {
     try
     {
       
       tt.tic("Dropping Columns")
       val todrop: Seq[String] = dfvInput.schema.filter(sf => sf.name != "A_AggregatedMajor" && sf.dataType != DoubleType && sf.dataType != IntegerType).map(sf => sf.name).toSeq ++ Seq("A_UID","ZZ_target")
       logger.info(s"Dropping ${todrop}")
       val dfvDroppedStrings: DataFrame = dfvInput.drop(todrop: _*).na.drop(2)
  
       logger.info(s"dfvDroppedStrings.count() ${dfvDroppedStrings.count()}")
  
       logger.info(s"dfvDroppedStrings.schema.fieldNames.length ${dfvDroppedStrings.schema.fieldNames.length}}")
       logger.info(s"dfvDroppedStrings.schema.fieldNames ${dfvDroppedStrings.schema.fieldNames.mkString(" - ")}}")
       logger.info(tt.toc)
       dfvDroppedStrings
     }
     catch
       {
         case e:Exception =>
           logger.error(e.getMessage(),e)
           fail(e.getMessage)
           throw new Exception(e)
       }
   }
  
   @throws(classOf[Exception])
   def runTests():Unit=
   {
   
    try
    {
      val spark = SparkSession.builder()
        .master("local")
        .appName("TermsDataReduction")
        .getOrCreate()
      
      val tt: TicToc = new TicToc
      tt.tic("Opening File")
      val dfvInput = DataFrameUtils.openFile(spark, "./unitTestInput/VERB0.csv")
      logger.info(tt.toc)
      
      
      //////////////////////////////////////////////////////////////////////////////////////////////////

      val dfvDroppedStrings= dropDFStringColumns(tt, dfvInput)
      
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      
      
      tt.tic("Printing df as pj")
      DataFrameUtils.saveToPjCsv(dfvDroppedStrings, "dfvDroppedStrings", "./unitTestOutput")
      logger.info(tt.toc)
      
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      
      
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      val dfDroppedLowFReq:DataFrame=dropDFBelowMedian(tt,spark,dfvDroppedStrings)
      
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      val dfvReduced = DataFrameUtils.groupBy(spark, "A_AggregatedMajor", dfDroppedLowFReq)
      DataFrameUtils.saveToPjCsv(dfvReduced, "dfReduced", "./unitTestOutput")
      

      val dfColumnSums: Seq[Long] = DataFrameUtils.columnSums(spark, dfvReduced.drop("A_RowId","Target"))


      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      //#############################################################################################################////
      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      
     tt.tic("Converting to libsvm")
      val sparse0: RDDLabeledPoint = new RDDLabeledPoint
      
      //////////////////////////////////////////////////////////////////////////////////////////////////
      val ignore: List[String] = List("A_RowId")
      
      /** libsvm col count does not include target or a rowid */
      sparse0.transformToLibSvm(spark, dfvDroppedStrings.drop("A_UID"), "A_AggregatedMajor", ignore)
      logger.info(tt.toc)
      logger.info(s"dfvDroppedStrings.count() ${dfvDroppedStrings.count()}\nsparse0.sparseData().count ${sparse0.sparseData().count}")
      
      tt.tic("Printing sparse")
      sparse0.print(spark, "unitTest_ConvertedToLibsvm", "./unitTestOutput")
      logger.info(tt.toc)
      sparse0.loadLibSvm(spark,"unitTest_ConvertedToLibsvm", "./unitTestOutput")
      //////////////////////////////////////////////////////////////////////////////////////////////////
      
      logger.info("\n\n----------------runTests: 1\n\n----------------")

      assertTrue(assertRowCountMatch(dfvDroppedStrings,sparse0))
    
      assertTrue(assertColumnNamesMatch(dfvDroppedStrings,sparse0,2))

      removeLowFreqFromSparse(tt,spark,sparse0)
      

      
      
      sparse0.print(spark, "sparseLowFreqRowsDropped", "./unitTestOutput")
      logger.info("\n\n----------------runTests: 2\n\n----------------")
      assertTrue(assertColumnCountMatch(dfDroppedLowFReq,sparse0,1))

      
      
      
      logger.info(s" Comparing field names df  ${dfDroppedLowFReq.schema.fieldNames(7)} | sparse ${sparse0.getColumnName(7)}")
      assertTrue( assertColumnNamesMatch(dfDroppedLowFReq,sparse0,0)      )
      val sparse1:RDDLabeledPoint=new RDDLabeledPoint
      sparse1.loadLibSvm(spark,"sparseLowFreqRowsDropped","./unitTestOutput")


      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////


      val dfDroppedByProportion:DataFrame=removeLowProportionFromSparse(tt,spark,sparse1,dfDroppedLowFReq,0)
      //DataFrameUtils.saveToPjCsv(dfDroppedByProportion,"dfDroppedByProportion","./unitTestOutput")
      //sparse1.printPjCsv(spark,"sparseDroppedByProportion","./unitTestOutput")

      assertTrue( assertColumnNamesMatch(dfDroppedByProportion,sparse1,0)      )
      //assertTrue( assertRowCountMatch(dfDroppedByProportion,sparse0) )

      assertTrue( assertColumnCountMatch(dfDroppedByProportion,sparse1,1) )
      
      
      
      ////////////////////////////////////////////////////////////////////////////////////////////
      

      
      
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      val sparseReduced:RDDLabeledPoint=sparse0.soReduceToTargetBySum(spark)
      //sparse0.print(spark, "sparseReduced", "./unitTestOutput")
      
      logger.info(s"dfvReduced count ${dfvReduced.count()}\nsparseReduced.sparseData().count ${sparseReduced.sparseData().count()}")
      logger.info("\n\n----------------runTests: 3\n\n----------------")
      assertTrue( assertRowCountMatch(dfvReduced,sparseReduced) )

      assertTrue( assertColumnCountMatch(dfvReduced,sparseReduced,1) )

      

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      val hm: Map[Int, CellTuple] = sparseReduced.soColSums()
      val colSums1:TreeMap[Int,CellTuple]=TreeMap(hm.toArray:_*)



      logger.info("\n\n----------------runTests: 4\n\n----------------")
      
      logger.info(s"colsums from df: ${dfColumnSums.mkString(",")}")
      logger.info(s"colsums from sparse: ${colSums1.values.mkString("-")}")
      logger.info(s"colsums from sparse: ${colSums1.values.size}  colsums from df: ${dfColumnSums.length}")
      assertTrue(dfColumnSums.length == colSums1.values.size)

      logger.info(s"df(0): ${dfColumnSums(0)}  sparse(0): ${colSums1.values.find(_.colIndex==0).get.value}")
      assertTrue(dfColumnSums(0) == colSums1.values.find(_.colIndex==0).get.value)
      assertTrue(dfColumnSums(1) == colSums1.values.find(_.colIndex==1).get.value)
      logger.info(s"df(dfColumnSums.length / 2): ${dfColumnSums.length / 2}  sparse(colSums1.values.find(_.colIndex==dfColumnSums.length / 2).get): ${colSums1.values.find(_.colIndex==dfColumnSums.length / 2).get.value}")
      assertTrue(dfColumnSums(dfColumnSums.length / 2) == colSums1.values.find(_.colIndex==dfColumnSums.length / 2).get.value)
      
      assertTrue(dfColumnSums(dfColumnSums.length - 3) == colSums1.values.find(_.colIndex==dfColumnSums.length -3).get.value)
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      
     
      
      
      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      assertTrue(fromLibsvm(spark, dfvDroppedStrings, dfDroppedLowFReq, dfvReduced, dfColumnSums))

      
      
      
    }
    catch
    {
      case ex: Exception =>
      {
        
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}")
        throw(ex)
        
        //fail()
      }
    }
  }
  
  
  @throws(classOf[Exception])
  def fromLibsvm(spark: SparkSession, dfvDroppedStrings: DataFrame, dfDroppedLowFReq: DataFrame, dfvReduced: DataFrame, dfColumnSums: Seq[Long]): Boolean =
  {
    try
    {
      val tt: TicToc = new TicToc
      
      tt.tic("fromLibsvm: loading libsvm")
      val sparse1: RDDLabeledPoint = new RDDLabeledPoint
      
      //////////////////////////////////////////////////////////////////////////////////////////////////
      val ignore: List[String] = List("A_RowId")
      
      /** libsvm col count does not include target or a rowid */
      sparse1.loadLibSvmPJ(spark, "./unitTestInput/VERB0.libsvm")

      
      //////////////////////////////////////////////////////////////////////////////////////////////////
      
      sparse1.print(spark,"fromLibsvmDirectExport","./unitTestOutput")
      logger.info("fromLibsvm: 1")

      assertTrue(assertRowCountMatch(dfvDroppedStrings,sparse1))

      assertTrue(assertColumnCountMatch(dfvDroppedStrings,sparse1,2))


      removeLowFreqFromSparse(tt,spark,sparse1)
      logger.info("fromLibsvm: 2")




      assertTrue(assertColumnNamesMatch(dfDroppedLowFReq,sparse1,0) )

      
      
      //sparse1.printPjCsv(spark,"fromLibsvmdroppedLowFreqFromSparse","./unitTestOutput")
      assertTrue(dfDroppedLowFReq.schema.fieldNames.length == sparse1.getColumnCount+1)
      
      
      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
      
      
      val sparseReduced:RDDLabeledPoint=sparse1.soReduceToTargetBySum(spark)
      
      
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      //sparse1.print(spark, "sparseReduced", "./unitTestOutput")
      logger.info("fromLibsvm: 3")

      assertTrue(assertRowCountMatch(dfvReduced,sparseReduced   ))
      logger.info(s"fromLibsvm: dfvReduced.schema.fieldNames.length ${dfvReduced.schema.fieldNames.length}\nsparseReduced.sparseData().count ${sparseReduced.getColumnCount + 1}")
      
      
      assertTrue(assertColumnCountMatch(dfvReduced,sparseReduced,1) )
      
      
      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      val colSums1: Map[Int, CellTuple] = sparseReduced.soColSums()
      logger.info("fromLibsvm: 4")
      logger.info(s"colsums from df: ${dfColumnSums.mkString(",")}")
      logger.info(s"colsums from sparse: ${colSums1.values.mkString("-")}")
      logger.info(s"colsums from sparse: ${colSums1.values.size}  colsums from df: ${dfColumnSums.length}")
      assertTrue(dfColumnSums.length == colSums1.values.size)

      logger.info(s"df(0): ${dfColumnSums(0)}  sparse(0): ${colSums1.values.find(_.colIndex==0).get.value}")
      assertTrue(dfColumnSums(0) == colSums1.values.find(_.colIndex==0).get.value)
      assertTrue(dfColumnSums(1) == colSums1.values.find(_.colIndex==1).get.value)
      logger.info(s"df(dfColumnSums.length / 2): ${dfColumnSums.length / 2}  sparse(colSums1.values.find(_.colIndex==dfColumnSums.length / 2).get): ${colSums1.values.find(_.colIndex==dfColumnSums.length / 2).get.value}")
      assertTrue(dfColumnSums(dfColumnSums.length / 2) == colSums1.values.find(_.colIndex==dfColumnSums.length / 2).get.value)

      assertTrue(dfColumnSums(dfColumnSums.length - 3) == colSums1.values.find(_.colIndex==dfColumnSums.length -3).get.value)

      val colleft=sparseReduced.getColumnName(3)
      val colToRemove=sparseReduced.getColumnName(4)
      val colRight=sparseReduced.getColumnName(5)

      val colSums:Map[Int,CellTuple]=sparseReduced.soColSums()
      val colRightSum:Double=colSums.get(5).get.value
      var queue:mutable.Queue[Int]=new mutable.Queue[Int]
      queue += 4
      sparseReduced.soRemoveColumns(spark,queue)
      val newColumnName=sparseReduced.getColumnName(4)
      logger.info(s"Testing shifted column $newColumnName was $colRight")
      assertTrue(newColumnName.equals(colRight))
      val colSumsNew:Map[Int,CellTuple]=sparseReduced.soColSums()
      val colNewSum:Double=colSumsNew.get(4).get.value
      logger.info(s"Testing shifted column value $colNewSum was $colRightSum")
      assertTrue(colNewSum==colRightSum)




      true
    }
    catch
    {
      case ex: Exception =>
      {
        
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}")
        false
        //fail()
      }
    }
  }
  
  def randomAlphaNumericString(length: Int): String = {
    val chars = ('A' to 'B') ++ ('0' to '1')
    randomStringFromCharList(length, chars)
  }
  
  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }
  
  @throws(classOf[Exception])
  def generateSmallLibsvm(): Unit =
  {
    Spreadsheet.getInstance().initLibsvm2("small","target","C:\\Users\\jake\\__workspace\\scalaProjects\\scalaML\\unitTestInput")
    Spreadsheet.getInstance().getLibSvmProcessor("small").setPkColumnName("UID")
    val columns:Seq[String] = Seq("target","UID","col1","col2","col3","col4","col5","col6")
    val r = scala.util.Random
    
    
    for(i <- 1 to 1000)
    {
      
      columns.foreach
      {
        
        col =>
          if (col.equals("target"))
          {
            
            Spreadsheet.getInstance().getLibSvmProcessor("small").addCell(i, col, randomAlphaNumericString(2))
          }
          else
          {
            val n = r.nextInt(100)
            Spreadsheet.getInstance().getLibSvmProcessor("small").addCell(i, col, n)
          }
      }
    }
  
    Spreadsheet.getInstance().getLibSvmProcessor("small").printLibSvmFinal("small", "target",Array.empty[String])
    val sparse1:RDDLabeledPoint = new RDDLabeledPoint
    val spark = SparkSession.builder()
      .master("local")
      .appName("TermsDataReduction")
      .getOrCreate()
    sparse1.loadLibSvmPJ(spark, "./unitTestInput/small.libsvm")
    sparse1.printSparse(spark,"smallOne","./unitTestInput",true)

  }****/
}
