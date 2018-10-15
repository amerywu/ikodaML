package ikoda.ml

import grizzled.slf4j.Logging
import ikoda.ml.pipeline.IKodaItem
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.{SparkConfProviderWithStreaming}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert._
import org.junit._

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


@Test
class SparkTest extends Logging with SparkConfProviderWithStreaming
{
  /********************************
  var colsToDropFinal: ArrayBuffer[String] = ArrayBuffer[String]()
  var itemsMapFinal: List[Iterable[Any]] = List[Iterable[Any]]()
  val itemMapInput: mutable.HashMap[String, IKodaItem] = new mutable.HashMap[String, IKodaItem]()
  
  private def deletePartition(path: String): Unit =
  {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    
    
    try
    {
      hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
      println("....deleted....")
    }
    catch
    {
      case ex: Exception =>
      {
        println(s"There has been an Exception during deletion. Message is ${ex.getMessage} and ${ex}")
        
      }
    }
  }
  
  @throws(classOf[Exception])
  private def firstSVM(spark: SparkSession): Unit =
  {
    try
    {
      val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(spark.sparkContext, "src/test/resources/iris-libsvm.txt")
      val splits = data.randomSplit(Array(0.6, 0.4), seed = 123L)
      val training = splits(0).cache()
      val test = splits(1)
      val numIterations = 100
      val model = SVMWithSGD.train(training, numIterations)
      model.clearThreshold()
      val scoreAndLabels = test.map
      { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      val auROC = metrics.areaUnderROC()
      
      println("Area under ROC = " + auROC)
      deletePartition("output/svmmodeltest")
      
      model.save(spark.sparkContext, "output/svmmodeltest")
    }
    catch
    {
      case ex: Exception =>
      {
        throw new Exception(ex)
      }
    }
  }
  
  @throws(classOf[Exception])
  private def dataframeCreation(spark: SparkSession): Unit =
  {
    try
    {
      val irisRaw: RDD[String] = spark.sparkContext.textFile("src/test/resources/iris-in.txt")
      val rowRDD: RDD[Row] = irisRaw.map(p => Row.fromSeq(p.toSeq))
      println(s"irisRaw ${irisRaw.toDebugString}")
      println(s"rowRDD ${rowRDD.toDebugString}")
      
      val irisDF: DataFrame = spark.read.csv("src/test/resources/iris-in.txt")
      println(s"irisDF.count() ${irisDF.count()}")
      println(s"irisDF.columns.length ${irisDF.columns.length}")
      
      irisDF.show()
      val row1: Row = irisDF.first()
      val row1AsString: String = row1.getString(0)
      val colCount = row1AsString.count(_ == ' ') // returns 2
      println(s"colCount.getClass=${colCount.getClass}")
      
      val sqlContext = spark.sqlContext
      import sqlContext.implicits._
      
      // irisDF.write.format("libsvm").save("./output/iris.libsvm")
      
      val dfnew: DataFrame = irisDF.withColumn("_tmp", split($"_c0", " ")).select(
        $"_tmp".getItem(0).as("col1"),
        $"_tmp".getItem(1).as("col2"),
        $"_tmp".getItem(2).as("col3"),
        $"_tmp".getItem(3).as("col4")
      ).drop("_tmp")
      
      dfnew.show()
      
      
      val selectExprs =
      {
        0 until (colCount + 1) map (i => $"temp".getItem(i).as(s"col$i"))
      }
      
      
      val dfnew1: DataFrame = irisDF.withColumn("temp", split($"_c0", " ")).select(selectExprs: _*)
      
      dfnew1.show()
    }
    catch
    {
      case ex: Exception =>
      {
        throw new Exception(ex)
      }
    }
  }
  
  @throws(classOf[Exception])
  private def wordCount(sc: SparkContext): Unit =
  {
    try
    {
      val inputfile = sc.textFile("src/test/resources/text1.txt")
      val counts = inputfile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _);
      
      println(counts.toDebugString)
      counts.cache()
      counts.persist()
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      
      try
      {
        hdfs.delete(new org.apache.hadoop.fs.Path("./output/rddcount"), true)
        println("....deleted....")
      }
      catch
      {
        case ex: Exception =>
        {
          println(s"There has been an Exception during deletion. Message is ${ex.getMessage} and ${ex}")
          
        }
      }
      
      counts.saveAsTextFile("./output/rddcount");
    }
    catch
    {
      case ex: Exception =>
      {
        throw new Exception(ex)
      }
    }
  }
  
  
  def castAllTypedColumnsTo(df: DataFrame,
                            sourceType: DataType, targetType: DataType): DataFrame =
  {
    
    val columnsToBeCasted = df.schema
      .filter(s => s.dataType == sourceType)
    
    if (columnsToBeCasted.length > 0)
    {
      println(s"Found ${columnsToBeCasted.length} columns " +
        s"(${columnsToBeCasted.map(s => s.name).mkString(",")})" +
        s" - casting to ${targetType.typeName.capitalize}Type")
    }
    
    columnsToBeCasted.foldLeft(df)
    { (foldedDf, col) =>
      castColumnTo(foldedDf, col.name, targetType)
    }
  }
  
  private def changeColumnValue(spark: SparkSession, iDF: DataFrame, columnToShift: String, prefixToDrop: String, prefixToKeep: String): DataFrame =
  {
    try
    {
      import org.apache.spark.sql.functions._
      
      val columnToKeep: String = prefixToKeep + columnToShift.substring(prefixToDrop.length, columnToShift.length())
      
      logger.info(s"column to keep is ${columnToKeep}")
      val dfupdated: DataFrame = iDF.withColumn(columnToKeep, when(col(columnToKeep).isNull, col(columnToShift)).otherwise(col(columnToKeep)))
      
      
      dfupdated
    }
    catch
    {
      case e: Exception => logger.error(s"There has been an Exception. Message is ${e.getMessage} and ${e}")
        iDF
    }
  }
  
  @throws(classOf[Exception])
  def changeAllColumns(spark: SparkSession, df: DataFrame, columnsToShift: Seq[String], prefixToShift: String, prefixToKeep: String): DataFrame =
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
  
  private def castColumnTo(df: DataFrame, cn: String, tpe: DataType): DataFrame =
  {
    
    //println("castColumnTo")
    df.withColumn(cn, df(cn).cast(tpe)
    
    )
  }
  
  @throws(classOf[Exception])
  private def convertRowToLabeledPoint(rowIn: Row, fieldNameSeq: Seq[String], label: Int): LabeledPoint =
  {
    try
    {
      val values: Map[String, Integer] = rowIn.getValuesMap(fieldNameSeq)
      
      val sortedValuesMap = ListMap(values.toSeq.sortBy(_._1): _*)
      
      //println(s"transformRowToLabeledPoint row values ${sortedValuesMap}")
      print(".")
      
      val rowValuesItr: Iterable[Integer] = sortedValuesMap.values
      
      var positionsArray: ArrayBuffer[Int] = ArrayBuffer[Int]()
      var valuesArray: ArrayBuffer[Double] = ArrayBuffer[Double]()
      var currentPosition: Int = 0
      rowValuesItr.foreach
      {
        kv =>
          if (kv > 0)
          {
            valuesArray += kv.doubleValue();
            positionsArray += currentPosition;
          }
          currentPosition = currentPosition + 1;
      }
      
      new LabeledPoint(label, org.apache.spark.mllib.linalg.Vectors.sparse(positionsArray.size, positionsArray.toArray, valuesArray.toArray))
      
      //println(s"valuesArray ${valuesArray}")
      //println(s"positionsArray ${positionsArray}")
      
      //println(lp)
      
      
    }
    catch
    {
      case ex: Exception =>
      {
        throw new Exception(ex)
      }
    }
  }
  
  @throws(classOf[Exception])
  private def convertToLibSvm(spark: SparkSession): Unit =
  {
    try
    {
      val mDF: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("src/test/resources/knimeMergedTrimmedVariables.csv")
      println(mDF.schema)
      val fieldSeq: scala.collection.Seq[StructField] = mDF.schema.fields.toSeq.filter(f => f.dataType == IntegerType)
      val fieldNameSeq: Seq[String] = fieldSeq.map(f => f.name)
      
      
      val indexer = new StringIndexer()
        .setInputCol("Majors_Final")
        .setOutputCol("Majors_Final_Indexed")
      val mDFTypedIndexed = indexer.fit(mDF).transform(mDF)
      val mDFFinal = castColumnTo(mDFTypedIndexed, "Majors_Final_Indexed", IntegerType)
      
      //mDFFinal.show()
      //only doubles accepted by sparse vector, so that's what we filter for
      
      
      var positionsArray: ArrayBuffer[LabeledPoint] = ArrayBuffer[LabeledPoint]()
      
      mDFFinal.collect().foreach
      {
        
        row => positionsArray += convertRowToLabeledPoint(row, fieldNameSeq, row.getAs("Majors_Final_Indexed"));
        
      }
      
      val mRdd: RDD[LabeledPoint] = spark.sparkContext.parallelize(positionsArray.toSeq)
      deletePartition("./output/libsvm")
      MLUtils.saveAsLibSVMFile(mRdd, "./output/libsvm")
    }
    catch
    {
      case ex: Exception =>
      {
        throw new Exception(ex)
      }
    }
  }
  
  @Test
  def testSpark(): Unit =
  {
    try
    {
      
      
      //val conf = new SparkConf().setAppName("test1").setMaster("local")
      
      //val sc = new SparkContext(conf)
      
      val spark = SparkSession.builder()
        .master("local")
        .appName("test")
        .getOrCreate()
      
      //spark.sparkContext.setLogLevel("INFO")
     // openFile(spark)
      
      // wordCount(spark.sparkContext)
      
      // dataframeCreation(spark)
      
      //firstSVM(spark)
      // transformToLibSvm(spark)
     // processiKodaScores(spark)
      
      
      assertTrue(true)
    }
    catch
    {
      case ex: Exception =>
      {
        
        logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}")
        fail(s"${ex.getMessage} ${ex.printStackTrace()}")
        //fail()
      }
    }
  }
  @throws(classOf[Exception])
  def openFile(spark:SparkSession): Unit =
  {
    try
    {
      val  sparse0:RDDLabeledPoint=new RDDLabeledPoint
      sparse0.loadLibSvmPJ(spark,"Z:\\_filedump\\collegeTestDump\\FINAL_C_trainingSet.libsvm")
  }
  catch
  {
    case ex: Exception =>
    {

      logger.error(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}")
      fail(s"${ex.getMessage} ${ex.printStackTrace()}")
      //fail()
    }
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
    
    logger.debug("asd;kljsa;ksadjfkl;asfjk';ls")
    
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
    val sb: StringBuilder = new StringBuilder
    itemMapInput.foreach
    {
      e =>
        sb.append("\n\n")
        sb.append(e._1)
        sb.append("    ->     ")
        sb.append(e._2)
      
    }
    logger.debug(sb)
    
  }
  
  
  def processiKodaScores(spark: SparkSession): Unit =
  {
    
    val iDF: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("src/test/resources/datafiles/iKodaScoresSortedSorted.csv")
    //iDF.printSchema()
    val iDFcached: DataFrame = iDF.cache
    
    val rddString: RDD[String] = spark.sparkContext.textFile(s"src/test/resources/datafiles/ikoda-items.csv")
    processiKodaItems(rddString)
    
    
    val dfDroppedDeleted = dropColumns(iDFcached)
    iDFcached.unpersist()
    dfDroppedDeleted.cache()
    
    val colsToShiftinSchema: Seq[StructField] = dfDroppedDeleted.schema.filter(f => f.name.startsWith("1279838_"))
    val colsNamesToShift: Seq[String] = colsToShiftinSchema.map(f => f.name)
    logger.info(s"colsNamesToShift: ${colsNamesToShift}")
    val dfMerged: DataFrame = changeAllColumns(spark, dfDroppedDeleted, colsNamesToShift, "1279838_", "1637_")
    
    dfDroppedDeleted.unpersist()
    val dfMergedDropped: DataFrame = dfMerged.drop(colsNamesToShift: _*)
    
    
    val dfMergedDroppedDeletedCached: DataFrame = dfMergedDropped.cache()
    
    dfMergedDroppedDeletedCached.printSchema()
    
    
    val dfNewNames = changeColHeaders1(dfMergedDroppedDeletedCached)
    
    logger.info(s"created dfNewNames with schema $dfNewNames")
    deletePartition("./output/iKodaResults.csv")
    logger.info(s"deleted partition")
    dfNewNames.coalesce(1).write.option("header", "true").csv("./output/iKodaResults.csv")
    logger.info(s"coalesced and saved")
    val dfScores: DataFrame = dfNewNames.drop(colsToDropFinal.toSeq: _*)
    deletePartition("./output/ikodaScores.csv")
    dfScores.coalesce(1).write.option("header", "true").csv("./output/ikodaScores.csv")
    dfMergedDroppedDeletedCached.unpersist()
    
    
    val rows: List[Row] = itemsMapFinal.map(x => Row(x.toSeq: _*))
    val rdd1: RDD[Row] = spark.sparkContext.makeRDD(rows)
    
    val colHeaders: Seq[String] = List("varName", "major", "itemType", "round", "dbId").toSeq
    
    
    val sqlContext = spark.sqlContext
    val schema = StructType(
      (1 to rdd1.first.size).map(i => StructField(s"_$i", StringType, false))
    )
    val tempdf = sqlContext.createDataFrame(rdd1, schema)
    val dfItemMap: DataFrame = tempdf.toDF(colHeaders: _*)
    
    
    val itemsdf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("src/test/resources/datafiles/ikodaMajorsItems.csv")
    
    
    val exclude: Seq[String] = List("contentelementtypepropertykey", "value", "v2", "dbId").toSeq
    val dfFinalItemMap = itemsdf.join(dfItemMap, itemsdf("ItemId") === dfItemMap("dbId")).drop(exclude: _*)
    
    
    deletePartition("./output/ikodaItems.csv")
    dfFinalItemMap.coalesce(1).write.option("header", "true").csv("./output/ikodaItems.csv")
    
    
  }
  
  
  def dropColumns(iDFcached: DataFrame): DataFrame =
  {
    val colsToDeleteinSchema: Seq[StructField] = iDFcached.schema.filter(f => f.name.startsWith("182025"))
    val moreColsToDeleteinSchema: Seq[StructField] = iDFcached.schema.filter(f => f.name.contains("response") || f.name.contains("startTime"))
    val colsNamesToDelete: Seq[String] = colsToDeleteinSchema.map(f => f.name)
    val moreColsToDelete: Seq[String] = moreColsToDeleteinSchema.map(f => f.name)
    
    val allColsToDelete: Seq[String] = colsNamesToDelete ++ moreColsToDelete
    val dfDroppedDeleted: DataFrame = iDFcached.drop(allColsToDelete: _*)
    dfDroppedDeleted
  }
  
  
  def changeColHeaders1(dfMergedDroppedDeletedCached: DataFrame): DataFrame =
  {
    val sb: StringBuilder = new StringBuilder
    
    
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
          logger.info("colName "+name)
          val singleItemMap: ArrayBuffer[String] = ArrayBuffer()
          logger.info(s"Column Number: $i")
          if (name.toUpperCase().contains("@SCORE"))
          {
            val fullId = name.substring(0, name.indexOf("_@SCORE"))
            val itemId = fullId.substring(fullId.lastIndexOf("_")+1, fullId.length)
            val major: String = "cet_property_cati_msproperties_assessed_category_A"
            logger.info(s"looking for $major with item id $itemId" )
            val itemType: String = "cet_property_cati_msproperties_assessed_category_B"
            logger.info(s"looking for $itemType")
            //val itemId: String = name.substring(0, name.indexOf("_@SCORE")) + "_@itemId"
            logger.info(s"looking for $itemId")
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
            }
            
            
            logger.info(s"got itemType as string ${itemTypestr}")
            //logger.info(majors)
            //logger.info(itemTypes)
            
            val itemIdInt: Integer = itemId.toInt
            logger.info(s"got itemType as int ${itemIdInt}")
            
            val round: String = itemTypestr.substring(itemTypestr.indexOf("R0") + 2, itemTypestr.indexOf("R0") + 3)
            logger.info("1")
            
            
            var subject: String = ""
            if (majorstr.length < 5)
            {
              subject = majorstr
            }
            else
            {
              subject = majorstr.substring(0, 3) + (majorstr.substring(majorstr.length - 2, majorstr.length))
            }
            logger.info("2")
            var itype: String = itemTypestr.substring(itemTypestr.lastIndexOf("_") + 1, itemTypestr.lastIndexOf("_") + 2)
            if (itype.toUpperCase().contains("I"))
            {
              itype = "f"
            }
            logger.info("3")
            val colName: String = itype + round + subject
            
            
            var tempstr = colName + 1
            var increment: Int = 1
            while (colMap.get(tempstr).isDefined)
            {
              
              increment += 1
              
              
              tempstr = colName + increment
            }
            
            colMap.put(tempstr, tempstr)
            logger.info(s"New colname is ${tempstr}")
            allColumnsRenamed += tempstr.toLowerCase()
            
            singleItemMap += tempstr.toLowerCase()
            singleItemMap += majorstr
            singleItemMap += itemTypestr
            singleItemMap += round
            singleItemMap += itemIdInt.toString
            
            itemsMap += singleItemMap
            logger.info(s"singleItemMap : $singleItemMap")
            
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
    logger.info(s"itemsMap : $itemsMap")
    logger.info(s"itemsMapFinal : $itemsMapFinal")
    
    
    logger.info("\n\n\nDONE\n\n\n\n")
    val dfNewNames: DataFrame = dfMergedDroppedDeletedCached.toDF(allColumnsRenamed.toSeq: _*)
    dfNewNames
  }
  
  ***********************************/
}


