package ikoda.ml.pipeline

import java.io.File

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.utilobjects.{DataFrameUtils, SparkConfProviderWithStreaming, UtilFunctions}
import ikoda.utils.Spreadsheet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/***
  * Calculates scores from the iKodaMajors assessment
  */
class IKodaScores extends Logging with SparkConfProviderWithStreaming with UtilFunctions with Serializable
{
  var colsToDropFinal: ArrayBuffer[String] = ArrayBuffer[String]()
  var itemsMapFinal: List[Iterable[Any]] = List[Iterable[Any]]()
  val itemMapInput: mutable.HashMap[String, IKodaItem] = new mutable.HashMap[String, IKodaItem]()
  val optionMapInput: mutable.HashMap[String, IKodaItem] = new mutable.HashMap[String, IKodaItem]()

  val sqlContext = spark.sqlContext


  def changeColHeaders(dfMergedDroppedDeletedCached: DataFrame): DataFrame =
  {
    val sb: StringBuilder = new StringBuilder
    logger.debug(s"changeColHeaders input df row count is ${dfMergedDroppedDeletedCached.count}")

    val allColNames: Seq[String] = dfMergedDroppedDeletedCached.schema.toSeq.map(f => f.name)
    val allColumnsRenamed: ArrayBuffer[String] = ArrayBuffer[String]()
    val colsToDrop: ArrayBuffer[String] = ArrayBuffer[String]()
    val itemsMap: ArrayBuffer[Iterable[Any]] = ArrayBuffer()

    var i: Int = 0
    val r = scala.util.Random


    val colMap: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap()

    allColNames.foreach
    {

      name =>
        try
        {
          logger.trace("colName " + name)
          val singleItemMap: ArrayBuffer[String] = ArrayBuffer()
          logger.trace(s"Column Number: $i")
          if (name.toUpperCase().contains("@SCORE"))
          {
            val fullId = name.substring(0, name.indexOf("_@SCORE"))
            val itemId = fullId.substring(fullId.lastIndexOf("_") + 1, fullId.length)
            val major: String = "cet_property_cati_msproperties_assessed_category_A"
            logger.trace(s"looking for $major with item id $itemId")
            val itemType: String = "cet_property_cati_msproperties_assessed_category_B"
            logger.trace(s"looking for $itemType")
            //val itemId: String = name.substring(0, name.indexOf("_@SCORE")) + "_@itemId"
            logger.trace(s"looking for $itemId")
            var itemTypestr: String = "NA"
            var majorstr: String = "NA"
            val o: Option[IKodaItem] = itemMapInput.get(itemId)

            if (o.isDefined)
            {
              val ii: IKodaItem = o.get
              val oit: Option[String] = ii.properties.get(itemType)
              val om: Option[String] = ii.properties.get(major)
              if (oit.isDefined)
              {
                itemTypestr = oit.get
              }
              if (om.isDefined)
              {
                majorstr = om.get
              }
              else
              {
                logger.warn(s"No major found for $name  with id $itemId");
              }
            }


            logger.trace(s"ITEM TYPE:  ${itemTypestr} MAJOR: ${majorstr}")


            val itemIdInt: Integer = itemId.toInt
            logger.trace(s"got itemType as int ${itemIdInt}")

            val round: String = itemTypestr.substring(itemTypestr.indexOf("R0") + 2, itemTypestr.indexOf("R0") + 3)

            if (majorstr == "NA")
            {
              logger.warn("!!!!No value for major!!!!")
            }


            var subject: String = ""
            if (majorstr.length < 5)
            {
              subject = majorstr
            }
            else
            {
              subject = majorstr.substring(0, 3) + (majorstr.substring(majorstr.length - 2, majorstr.length))
            }

            var itype: String = itemTypestr.substring(itemTypestr.lastIndexOf("_") + 1,
              itemTypestr.lastIndexOf("_") + 2)
            if (itype.toUpperCase().contains("I"))
            {
              itype = "f"
            }

            val colName: String = itype + round + subject


            var tempstr = colName + 1
            var increment: Int = 1
            while (colMap.get(tempstr).isDefined)
            {

              increment += 1


              tempstr = colName + increment
            }

            colMap.put(tempstr, tempstr)
            logger.trace(s"New colname is ${tempstr}")
            allColumnsRenamed += tempstr.toLowerCase()

            singleItemMap += tempstr.toLowerCase()
            singleItemMap += majorstr
            singleItemMap += itemTypestr
            singleItemMap += round
            singleItemMap += itemIdInt.toString

            itemsMap += singleItemMap
            logger.trace(s"singleItemMap : $singleItemMap")

          }
          else
          {
            allColumnsRenamed += name

            colsToDrop += name
          }
          i = i + 1
        }
        catch
        {
          case e: Exception => logger.error(s"\n\n\n\n\n\n\n\n\n${e.getMessage}\n\n\n\n\n\n\n\n\n\n")
            allColumnsRenamed += name
            colsToDrop += name
        }

    }


    colsToDropFinal = colsToDrop.filter(s => !s.contains("RowId"))
    itemsMapFinal = itemsMap.toList
    logger.info(s"itemsMap size: ${itemsMap.size}")
    logger.info(s"itemsList size: ${itemsMapFinal.size}")


    val dfNewNames: DataFrame = dfMergedDroppedDeletedCached.toDF(allColumnsRenamed.toSeq: _*)


    logger.info("\n\n\nDONE\n\n\n\n")
    dfNewNames
  }

  private def changeColumnValue(spark: SparkSession, iDF: DataFrame, columnToShift: String, prefixToDrop: String,
                                prefixToKeep: String): DataFrame =
  {
    try
    {
      import org.apache.spark.sql.functions._

      val columnToKeep: String = prefixToKeep + columnToShift.substring(prefixToDrop.length, columnToShift.length())

      logger.info(s"column to keep is ${columnToKeep}")
      val dfupdated: DataFrame = iDF.withColumn(columnToKeep,
        when(col(columnToKeep).isNull, col(columnToShift)).otherwise(col(columnToKeep)))


      dfupdated
    }
    catch
    {
      case e: Exception => logger.error(s"There has been an Exception. Message is ${e.getMessage} and ${e}")
        iDF
    }
  }

  @throws(classOf[Exception])
  def changeAllColumns(spark: SparkSession, df: DataFrame, columnsToShift: Seq[String], prefixToShift: String,
                       prefixToKeep: String): DataFrame =
  {
    try
    {
      print("__")
      columnsToShift.foldLeft(df)
      { (foldedDf: DataFrame, colname: String) =>
        changeColumnValue(spark, foldedDf, colname, prefixToShift: String, prefixToKeep)
      }
    }
    catch
    {
      case ex: Exception => throw new Exception(ex)
    }
  }


  def processiKodaItems(rddString: RDD[String]): Unit =
  {
    itemMapInput.clear()
    val parsedList: Seq[(String, String, String, String, String)] = rddString.map
    { record =>

      val a = record.replaceAll("\"", "").split(",")
      val id = a(0)
      val prompt = a(1)
      val stem = a(2)
      val propertyName = a(3)
      val propertyValue = a(4)

      (id, prompt, stem, propertyName, propertyValue)
    }.collect().toSeq


    parsedList.foreach
    {
      tuple =>
        val option = itemMapInput.get(tuple._1)
        if (option.isDefined)
        {
          val ii: IKodaItem = option.get
          ii.properties.put(tuple._4, tuple._5)
        }
        else
        {
          val props: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
          props.put(tuple._4, tuple._5)
          val iinew: IKodaItem = new IKodaItem(tuple._1, tuple._2, tuple._3, props)
          itemMapInput.put(tuple._1, iinew)
        }
    }
  }


  def processiKodaOption(rddString: RDD[String]): Unit =
  {
    logger.debug("processiKodaOption input rdd " + rddString.count)
    optionMapInput.clear()
    val parsedList: Seq[(String, String, String, String)] = rddString.map
    { record =>

      val a = record.replaceAll("\"", "").split(",")
      val optionid = a(0)
      val option = a(1)
      val mcitemid = a(2)
      val code = a(3)
      (optionid, option, mcitemid, code)
    }.collect().toSeq

    logger.debug("processiKodaOption " + parsedList.mkString("\n"))


    parsedList.foreach
    {
      tuple =>

        val props: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
        val iinew: IKodaItem = new IKodaItem(tuple._1, tuple._2, tuple._4, props)
        optionMapInput.put(tuple._1, iinew)

    }
    logger.debug("processiKodaOption " + optionMapInput.mkString("|\n|"))
  }


  def dirContains(dir: String, s: String): Boolean =
  {
    val d = new File(dir)
    if (d.exists && d.isDirectory)
    {
      d.listFiles.filter(_.getName.contains(s)).toList.size > 0
    }
    else
    {
      false
    }
  }


  @Override
  override def getListOfFiles(dir: String): List[String] =
  {
    val d = new File(dir)
    if (d.exists && d.isDirectory)
    {
      d.listFiles.filter(_.isFile).map(f => f.getAbsolutePath.toString).toList
    }
    else
    {
      List[String]()
    }
  }


  @throws
  def dataCompileRowBlocks(path: String, uidColName: String, columnsToIgnore: List[String], fileNames: String*): Unit =
  {
    try
    {
      logger.debug(" dataCompileRowBlocks ")
      val fileNamesList: List[String] = fileNames.toList
      logger.debug(fileNamesList.mkString(","))
      if (dirContains(path, "FINAL"))
      {
        throw new Exception("FINAL file already in path....aborting");
      }


      fileNamesList.foreach
      {
        filename =>
          Spreadsheet.getInstance().initCsvSpreadsheet(filename, "ikoda.ml.", path)
          Spreadsheet.getInstance().getCsvSpreadSheet(filename).setPkColumnName(uidColName)
          Spreadsheet.getInstance().getCsvSpreadSheet(filename).finalizeAndJoinBlocks(filename, false)
          Spreadsheet.getInstance().getCsvSpreadSheet(filename).printCsvFinal("ikMajors")
          Spreadsheet.getInstance().getCsvSpreadSheet(filename).clearAll()
      }

      logger.info("Data files combined.")
    }
    catch
    {
      case e: Exception =>
      {
        logger.error(e.getMessage, e)
        throw new Exception(e)
      }
    }
  }


  @throws(classOf[IKodaMLException])
  def demographicsCodes(spark: SparkSession, dfdem: DataFrame): DataFrame =
  {
    try
    {
      import org.apache.spark.sql.functions._
      val newColValue = udf
      { (s: String) =>

        val o = optionMapInput.get(s)

        o.isDefined match
        {
          case true => o.get.prompt
          case false => s + "_unknown"
        }
      }

      dfdem
        .columns.filter(cn => !cn.toLowerCase().contains("rowid"))
        .foldLeft(dfdem)
        { (inDF, colName) =>
          inDF
            .withColumn(
              colName,
              newColValue(inDF(colName))
            )
        }
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }


  }


  def demographicsColHeads(spark: SparkSession, dfdem: DataFrame): DataFrame =
  {


    val rddString: RDD[String] = spark.sparkContext.textFile(s"./input/ikoda-demographicItems.csv")
    val itemMap: Map[String, String] = rddString.collect().map
    {
      row =>

        logger.debug(row)
        val split = row.replace("\"", "").split(",")

        (split(0), split(1))


    }.toMap

    logger.info("Demographics Item Map \n" + itemMap.mkString("\n"))
    logger.info("dfdem.schema.fieldNames \n" + dfdem.schema.fieldNames.mkString("\n"))
    val colNames: Seq[String] = dfdem.schema.fieldNames.map
    {
      r =>
        val trimmed = r.replace("182025", "").replace("_0", "").replace("response", "").replace("_", "")
        logger.debug(trimmed)

        val newColName = itemMap.get(trimmed)
        logger.debug(newColName)
        newColName.isDefined match
        {
          case true =>
            val colhead = newColName.get.toLowerCase.replace(" ", "").replace("?", "")
            logger.debug("colhead " + colhead)
            if (colhead.length > 18)
            {

              colhead.substring(0, 17)
            }
            else if (colhead.contains("Row"))
            {
              "A_RowId"
            }
            else
            {
              colhead
            }
          case _ => trimmed
        }
    }

    logger.info("New column names " + colNames.mkString(", "))

    require(dfdem.schema.fieldNames.length == colNames.length)

    val dfUpdatedColHeads = dfdem.toDF(colNames: _*)
    logger.debug(dfUpdatedColHeads.schema.mkString("  |  "))
    dfUpdatedColHeads


  }

  def demographicsDF(spark: SparkSession, iDFcached: DataFrame): DataFrame =
  {


    val colsToKeepinSchema: Seq[StructField] = iDFcached.schema.filter(
      f => f.name.startsWith("182025") || f.name.toLowerCase().contains("a_rowid"))
    val colsNamesToKeep: Seq[String] = colsToKeepinSchema.map(f => f.name)
    val dfnew: DataFrame = iDFcached.select(colsNamesToKeep.head, colsNamesToKeep.tail: _*)
    val dfwithschema = dfnew.toDF(colsNamesToKeep: _*)


    val moreColsToDeleteinSchema: Seq[StructField] = iDFcached.schema
      .filter(f =>
        f.name.contains("SCORE") ||
          f.name.contains("startTime") ||
          f.name.contains("longresponse") ||
          f.name.contains("item_pool_id") ||
          f.name.contains("@itemId") ||
          f.name.contains("duration")


      )

    val moreColsToDelete: Seq[String] = moreColsToDeleteinSchema.map(f => f.name)


    val dfout = dfwithschema.drop(moreColsToDelete: _*)

    dfout


  }


  def dropColumns(spark: SparkSession, iDFcached: DataFrame): DataFrame =
  {

    val colsToDeleteinSchema: Seq[StructField] = iDFcached.schema.filter(f => f.name.startsWith("182025"))
    val moreColsToDeleteinSchema: Seq[StructField] = iDFcached.schema.filter(
      f => f.name.contains("response") || f.name.contains("startTime"))
    val colsNamesToDelete: Seq[String] = colsToDeleteinSchema.map(f => f.name)
    val moreColsToDelete: Seq[String] = moreColsToDeleteinSchema.map(f => f.name)

    val allColsToDelete: Seq[String] = colsNamesToDelete ++ moreColsToDelete
    val dfDroppedDeleted: DataFrame = iDFcached.drop(allColsToDelete: _*)
    dfDroppedDeleted
  }


  def deleteIncompleteTests(path: String): DataFrame =
  {
    val df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    deleteIncompleteTests(df)
  }

  def deleteIncompleteTests(df: DataFrame): DataFrame =
  {

    val sparksess = getSparkSession()
    import sparksess.sqlContext.implicits._

    df.filter($"A_Final_Status" === "TSICompleted")

  }

  @throws(classOf[Exception])
  def shiftColumns(spark: SparkSession, dfDroppedDeleted: DataFrame): DataFrame =
  {
    try
    {
      val colsToShiftinSchema: Seq[StructField] = dfDroppedDeleted.schema.filter(f => f.name.startsWith("1279838_"))
      val colsNamesToShift: Seq[String] = colsToShiftinSchema.map(f => f.name)
      logger.info(s"colsNamesToShift: ${colsNamesToShift}")
      val dfMerged: DataFrame = changeAllColumns(spark, dfDroppedDeleted, colsNamesToShift, "1279838_", "1637_")

      dfDroppedDeleted.unpersist()
      val dfMergedDropped: DataFrame = dfMerged.drop(colsNamesToShift: _*)
      dfMergedDropped.cache()
    }
    catch
    {
      case e: Exception => throw new Exception(e.getMessage, e)
    }
  }


  @throws
  def renameCols(fileName: String, dir: String): Unit =
  {
    try
    {
      logger.info("Merging Columns")
      Spreadsheet.getInstance().initCsvSpreadsheet("colShift", "ikoda.ml", dir)
      Spreadsheet.getInstance().getCsvSpreadSheet("colShift").loadCsv(fileName, "A_RowId");
      val colnames: Array[String] = Spreadsheet.getInstance().getCsvSpreadSheet("colShift").getColumnNames


      val colSeq = colnames.toSeq


      colnames.toSeq.foreach
      {
        col =>

          if (col.startsWith("1637_"))
          {
            val colTruncated = col.substring(5, col.length)

            val newColName: String = "1279838_" + colTruncated
            logger.debug(newColName)
            Spreadsheet.getInstance().getCsvSpreadSheet("colShift").renameColumn(col, newColName)
          }
      }

      Spreadsheet.getInstance().getCsvSpreadSheet("colShift").printCsvFinal("cleanedData")
      Spreadsheet.getInstance().getCsvSpreadSheet("colShift").clearAll()

      logger.info("Columns Merged")


    }
    catch
    {
      case ex: Exception =>
      {

        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}", ex)
        throw new Exception(ex);
        //fail()
      }
    }
  }


  def processKey(fieldName: String): String =
  {
    val key = fieldName.substring(2, fieldName.length - 1)
    logger.trace(s"processLikertScore key: ${key}")
    key

  }


  def finalScoreDf(spark: SparkSession, dfRenamedTrimmedCols: DataFrame): Option[DataFrame] =
  {
    try
    {
      logger.info("finalScoreDf")
      val hmAllRowScores: Map[Long, mutable.HashMap[String, Tuple2[Long, ListBuffer[Double]]]] = finalScore(
        dfRenamedTrimmedCols)
      logger.info(s"\n--------------------\nhmAllRowScores size ${hmAllRowScores.size}\n--------------------")
      val lbFinalScores: ListBuffer[Tuple3[Long, String, Double]] = ListBuffer()

      hmAllRowScores.foreach
      {
        r =>
          var maxValue: Double = 0.0
          var major: String = ""
          logger.trace(s"${r._1} : ${r._2}")
          val rowId: Long = extractLong(r._1).get
          val allMajors: mutable.HashMap[String, Tuple2[Long, ListBuffer[Double]]] = r._2
          allMajors.foreach
          {
            m =>
              val lb: ListBuffer[Double] = m._2._2
              var score: Double = lb.toList.sum
              if (score > maxValue)
              {
                maxValue = score
                major = m._1

              }
          }
          lbFinalScores += new Tuple3(rowId, major, maxValue)


      }

      val sqlContext = spark.sqlContext
      import sqlContext.implicits._

      Some(lbFinalScores.toDF("A_RowId", "Final Choice", "Top Score"))
    }
    catch
    {
      case e: Exception => logger.error(e.getMessage, e)
        None
    }


  }

  @throws(classOf[Exception])
  def finalScore(df: DataFrame): Map[Long, mutable.HashMap[String, Tuple2[Long, ListBuffer[Double]]]] =
  {
    try
    {

      logger.debug(df.schema.fields.mkString("-\n-"))

      logger.info(s"Collecting df to calculate final scores row count ${df.count}")
      df.collect().map
      {
        r =>

          val hm: mutable.HashMap[String, Tuple2[Long, ListBuffer[Double]]] = new mutable.HashMap()
          r.getValuesMap(df.schema.fieldNames).foreach
          {

            vm =>

              //logger.debug(s"Field Name in Results Schema   n ${vm._1} v ${vm._2} ")
              val key: String = processKey(vm._1)
              val scoreAny = r.getAs[Any](vm._1)

              var scoreo: Option[Double] = extractDouble(scoreAny)
              // logger.debug(s"Row: ColName is: ${vm._1}.  Major is $key. scoreAny is $scoreAny. Scoreo is $scoreo")
              if (scoreo.isDefined)
              {
                var score: Double = scoreo.get
                if (vm._1.startsWith("f"))
                {
                  score = score * 3
                }
                val tuple: Tuple2[Long, ListBuffer[Double]] = hm.getOrElse(key,
                  new Tuple2(r.getAs[Long]("A_RowId"), new ListBuffer[Double]))
                tuple._2 += score
                if (!key.toUpperCase.contains("ROWI"))
                {
                  hm.put(key, tuple)
                }
              }
          }
          val rowId = extractLong(r.getAs[Long]("A_RowId")).get
          logger.debug(s"one row score $rowId ${hm.mkString(" \n\n ")}")


          rowId -> hm


      }.toMap

    }
    catch
    {
      case e: Exception => throw new Exception(e.getMessage, e)
    }
  }


  def rddStringToDFWithSchema(rdd: RDD[String]): DataFrame =
  {
    val schema = new StructType(rdd.first.split(",").map
    {
      fn => StructField(fn, StringType, true)
    })
    val header = rdd.first()
    val array: Array[Array[String]] = rdd.collect.filter(row => row != header).map
    {
      r => r.split(",")
    }
    val sparksess = getSparkSession()
    val rdd1 = spark.sparkContext.parallelize(array)

    import sparksess.sqlContext.implicits._
    rdd1.toDF(schema.fieldNames.toSeq: _*)
  }


  @throws(classOf[IKodaMLException])
  def processiKodaScores(pathToFile: String): Unit =
  {

    try
    {
      val iDF: DataFrame = spark.read.option("header", "true").csv(pathToFile)
      //iDF.printSchema()
      val iDFcached: DataFrame = iDF.cache

      val idFiltered = deleteIncompleteTests(iDFcached)
      idFiltered.cache
      val rddString: RDD[String] = spark.sparkContext.textFile(s"./input/ikoda-items.csv")
      val rddItemsString: RDD[String] = spark.sparkContext.textFile(s"./input/ikoda-items.csv")


      processiKodaItems(rddItemsString)


      val rddOptionsString: RDD[String] = spark.sparkContext.textFile(s"./input/ikoda-demographicOptions.csv")


      processiKodaOption(rddOptionsString)

      val dfDemographics: DataFrame = demographicsDF(spark, idFiltered)


      DataFrameUtils.deletePartition("./output/ikoda-demographicso.csv")
      logger.info(s"deleted partition")
      dfDemographics.coalesce(1).write.option("header", "true").csv("./output/ikoda-demographicso.csv")
      logger.info(s"coalesced and saved")


      val dfDemographicsRenamed = demographicsColHeads(spark, dfDemographics)


      DataFrameUtils.logRows(dfDemographicsRenamed, 0.0005)

      val dfDemographicsCoded = demographicsCodes(spark, dfDemographicsRenamed)

      DataFrameUtils.deletePartition("./output/ikoda-demographics.csv")
      logger.info(s"deleted partition")
      dfDemographicsCoded.coalesce(1).write.option("header", "true").csv("./output/ikoda-demographics.csv")
      logger.info(s"coalesced and saved")


      val dfDroppedDeleted = dropColumns(spark, idFiltered)

      idFiltered.unpersist()
      dfDroppedDeleted.cache()


      logger.info(s"Merged Columns. Row count is ${dfDroppedDeleted.count()}")

      val dfNewNames = changeColHeaders(dfDroppedDeleted)


      logger.info(s"created dfNewNames with schema $dfNewNames and rowCount of ${dfNewNames.count()}")

      DataFrameUtils.deletePartition("./output/iKodaResults.csv")
      logger.info(s"deleted partition")
      dfNewNames.coalesce(1).write.option("header", "true").csv("./output/iKodaResults.csv")
      logger.info(s"coalesced and saved")
      val dfRenamedTrimmedCols: DataFrame = dfNewNames.drop(colsToDropFinal.toSeq: _*)

      DataFrameUtils.deletePartition("./output/dfRenamedTrimmedCols.csv")
      dfRenamedTrimmedCols.coalesce(1).write.option("header", "true").csv("./output/dfRenamedTrimmedCols.csv")
      logger.info(s"dfRenamedTrimmedCols row count ${dfRenamedTrimmedCols.count()}")

      val dfDevianceFromMean = devianceFromAverage(dfRenamedTrimmedCols)
      DataFrameUtils.deletePartition("./output/dfDevianceFromMean.csv")
      dfDevianceFromMean.coalesce(1).write.option("header", "true").csv("./output/dfDevianceFromMean.csv")

      DataFrameUtils.logRows(dfDevianceFromMean, 0.001, "deviance from mean")


      val dfTopChoiceo: Option[DataFrame] = finalScoreDf(spark, dfRenamedTrimmedCols)

      if (dfTopChoiceo.isDefined)
      {

        DataFrameUtils.deletePartition("./output/finalOriginalScores.csv")
        DataFrameUtils.deletePartition("./output/finalDevianceFromMean.csv")
        finalScoresDataFrame("finalOriginalScores",dfRenamedTrimmedCols,dfTopChoiceo.get,dfDemographicsCoded)
        finalScoresDataFrame("finalDevianceFromMean",dfDevianceFromMean,dfTopChoiceo.get,dfDemographicsCoded)
        dfDroppedDeleted.unpersist()


        val rows: List[Row] = itemsMapFinal.map(x => Row(x.toSeq: _*))
        val rdd1: RDD[Row] = spark.sparkContext.makeRDD(rows)

        val colHeaders: Seq[String] = List("varName", "major", "itemType", "round", "dbId").toSeq


        val sqlContext = spark.sqlContext
        val schema = StructType(
          (1 to rdd1.first.size).map(i => StructField(s"_$i", StringType, false))
        )
        val tempdf = sqlContext.createDataFrame(rdd1, schema)
        val dfItemMap: DataFrame = tempdf.toDF(colHeaders: _*)


        val itemsdf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(
          "./input/ikodaMajorsItems.csv")


        val exclude: Seq[String] = List("contentelementtypepropertykey", "value", "v2", "dbId").toSeq
        val dfFinalItemMap = itemsdf.join(dfItemMap, itemsdf("ItemId") === dfItemMap("dbId")).drop(exclude: _*)


        DataFrameUtils.deletePartition("./output/ikodaItems.csv")
        dfFinalItemMap.coalesce(1).write.option("header", "true").csv("./output/ikodaItems.csv")


      }
    }
    catch
      {
        case e:Exception => logger.error(e.getMessage,e)
          logger.error(e.getCause)
          throw new IKodaMLException(e.getMessage,e)
      }


  }

  @throws(classOf[IKodaMLException])
  def finalScoresDataFrame(fileName:String, scoresDf:DataFrame,topChoiceDf:DataFrame, demographicsDf:DataFrame): Unit =
  {
    try
    {

      logger.info(s"dfTopChoice row count ${topChoiceDf.count()}")
      val dfScoresWithDemographics = DataFrameUtils.join(scoresDf, demographicsDf, "A_RowId")
      val dfScoresFinal = DataFrameUtils.join(dfScoresWithDemographics, topChoiceDf, "A_RowId")
      logger.info(s"dfScoresFinal row count ${dfScoresFinal.count()}")

      DataFrameUtils.deletePartition(s"./output/$fileName.csv")
      dfScoresFinal.coalesce(1).write.option("header", "true").csv(s"./output/$fileName.csv")
    }
    catch
      {
        case e:Exception=> logger.error(e.getMessage,e)
          throw new IKodaMLException(e.getMessage,e)
      }
  }




  def splitByRoundLikertOnly(dfScoresFinal: DataFrame):Seq[DataFrame]=
  {
    val likertDf=splitByRound(dfScoresFinal,"l")
    Seq(splitByRound(dfScoresFinal,"l1"),splitByRound(dfScoresFinal,"l2"),splitByRound(dfScoresFinal,"l3"))
  }

  def splitByRound(df:DataFrame, colPrefix:String):DataFrame=
  {
    val colsToKeepl1Seq = df.schema.fieldNames.filter(
      fn => fn.startsWith(colPrefix) || fn.toLowerCase().startsWith("a_row"))
    df.select(colsToKeepl1Seq.head, colsToKeepl1Seq.tail: _*)
  }

  def averageForRoundRowWise(df:DataFrame, round:Int): DataFrame =
  {


    val avg = (sum:Double, length:Int) =>
      {
        length match
          {
          case x if (x >0) => sum / length
          case _ => -9999
        }
      }

    val sparsess=getSparkSession()
    import sparsess.implicits._
    df.map
    {
      row =>
        val arrayBuffer: ArrayBuffer[Double] = new ArrayBuffer[Double]()

        //.filter(fn => !fn.toLowerCase().startsWith("a_row"))
        row.toSeq.foreach
        {
          v =>

            val vo = extractDouble(v)

            vo.isDefined match
            {
              case true =>
                if (vo.get >= 0 && vo.get < 4)
                {
                  arrayBuffer += vo.get
                }
              case false =>
            }
        }


        logger.debug("row average: "+avg)
        (extractString(row.getAs[Long]("A_RowId")), avg(arrayBuffer.sum,arrayBuffer.length))
      //("cvxb","cvb")
    }.toDF("A_RowId", "Average"+round)
  }

  @throws(classOf[IKodaMLException])
  def devianceFromAverageOneRound(df:DataFrame, round:Int):DataFrame=
  {
    try
    {


      val dsAvg = averageForRoundRowWise(df,round)

      DataFrameUtils.logRows(df, 0.001, "likertscores"+round)
      DataFrameUtils.logRows(dsAvg, 0.001, "dsAvg"+round)


      val deviation = udf
      { (d: Double, avg: Double) =>


        d match
        {
          case d if (d>=0) =>
              val dev = d - avg
              logger.debug (d + " - " + avg + " = " + dev)
              dev
          case _ => -9999
        }
      }


      val newLik1Df = DataFrameUtils.join(dsAvg, df, "A_RowId")
      val dfdev = newLik1Df
        .columns.filter(cn => !cn.toLowerCase().contains("rowid") && !cn.toLowerCase().contains("avera"))
        .foldLeft(newLik1Df)
        { (inDF, colName) =>
          inDF
            .withColumn(
              colName,
              deviation(inDF(colName), inDF("Average"+round))
            )
        }

      DataFrameUtils.logRows(dfdev, 0.001, "dfdev")
      dfdev

    }
    catch
      {
        case e:Exception=> throw new IKodaMLException(e.getMessage,e)
      }
  }

  @throws(classOf[IKodaMLException])
  def devianceFromAverage(dfScoresFinal: DataFrame): DataFrame =
  {
    try
    {
      val likertRoundDfSeq:Seq[DataFrame]=splitByRoundLikertOnly(dfScoresFinal)
      var count=1
      likertRoundDfSeq match
      {
        case head :: tail => tail.foldLeft(devianceFromAverageOneRound(head, count)){(dforg,df1)=>
          count = count +1
          DataFrameUtils.join(dforg,devianceFromAverageOneRound(df1,count),"A_RowId")
        }
      }

    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


}
