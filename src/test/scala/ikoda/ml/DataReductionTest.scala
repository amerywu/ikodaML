package ikoda.ml

import java.io.File

import grizzled.slf4j.Logging
import ikoda.sparse.{ColumnHeadTuple, RDDLabeledPoint}
import ikoda.utilobjects.{DataFrameUtils, SparkConfProvider, SparkConfProviderWithStreaming}
import ikoda.utils.{Spreadsheet, TicToc}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel, LDA, LDAModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.FeatureType.{Categorical, Continuous}
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.rdd.RDD
import org.junit.Assert.fail
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DataReductionTest extends Logging with SparkConfProviderWithStreaming
{
/*******
  val nodes: ArrayBuffer[Node] = new ArrayBuffer[Node]()
  val wordsMap: mutable.HashMap[String, Int] = new mutable.HashMap()
  
  val termsToKeep:ArrayBuffer[Int]= new ArrayBuffer[Int]()

  
  

  def editCsvColumnTest(): Unit =
  {
    try
    {
      
      
      val ignoreList: List[String] = List("AA_Job Title", "AA_Major0", "AA_Major1", "AA_Major2", "AA_Major3",
        "AA_Major4", "AA_Major5", "AA_Major6", "AA_Major7", "AA_Major8", "AA_Major9", "AA_Major10", "AA_Major11"
        , "AA_Major12", "AA_Major13", "AA_Major14", "ZZ_TARGET")
      
      editCsvColumn("./unitTestInput",
        "MERGE3",
        "A_AggregatedMajor",
        "A_UID",
        " ",
        " ",
        ignoreList
      )
      
      
      editCsvColumn("./unitTestInput",
        "MERGE2",
        "A_AggregatedMajor",
        "A_UID",
        " ",
        " ",
        ignoreList
      )
      
      editCsvColumn("./unitTestInput",
        "MERGE1",
        "A_AggregatedMajor",
        "A_UID",
        " ",
        " ",
        ignoreList
      )
      
      
      /*editCsvColumn("E:\\outputS1_20000",
      "BLOCK",
      "A_AggregatedMajor",
      "A_UID",
      "_",
      " ",
      ignoreList
    )

    editCsvColumn("E:\\outputS2_50000",
      "BLOCK",
      "A_AggregatedMajor",
      "A_UID",
      "_",
      " ",
      ignoreList
    )

    editCsvColumn("E:\\outputS2_50000",
      "FINAL",
      "A_AggregatedMajor",
      "A_UID",
      "_",
      " ",
      ignoreList
    )*/
      
    
    }
    catch
    {
      case e: Exception => logger.error(e.getMessage, e)
        throw e
    }
    
  }
  
  
  def getListOfFiles(dir: String): List[String] =
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
  def editCsvColumn(path: String, coreFileName: String, columnName: String, uidCol: String, toReplace: String, replaceWith: String, ignoreList: List[String]): Unit =
  {
    try
    {
      
      val ignoreList: List[String] = List("AA_Job Title", "AA_Major0", "AA_Major1", "AA_Major2", "AA_Major3",
        "AA_Major4", "AA_Major5", "AA_Major6", "AA_Major7", "AA_Major8", "AA_Major9", "AA_Major10", "AA_Major11"
        , "AA_Major12", "AA_Major13", "AA_Major14", "ZZ_TARGET")
      
      val tt = new TicToc()
      
      val fileList: List[String] = getListOfFiles(path);
      
      
      
      fileList.filter(s => s.contains(coreFileName) && s.toUpperCase().contains(".CSV")).foreach
      {
        filepath =>
          
          logger.info(tt.tic(s"editing $filepath"))
          Spreadsheet.getInstance().initCsvSpreadsheet(coreFileName,path)
          Spreadsheet.getInstance().getCsvSpreadSheet(coreFileName).setTargetColumnName("A_AggregatedMajor")
          Spreadsheet.getInstance().getCsvSpreadSheet(coreFileName).changeCsvField(filepath, uidCol, columnName, toReplace, replaceWith, ignoreList.toArray)
          logger.info(tt.toc())
      }
      logger.info(tt.toc("Merging Files"))
      
      
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
  
  
  
  
  
  def isToKeep(s: String): Boolean =
  {
    if (s.equals("A_UID") || s.equals("A_AggregatedMajor"))
    {
      true
    }
    else
    {
      false
    }
  }
  
  
  def loadLibSvmPJ(): Unit =
  {
    
    
    val sparse0: RDDLabeledPoint = new RDDLabeledPoint()
    sparse0.loadLibSvmPJ(spark, "E:\\data\\BY_JOB_NOUN0_0_FINAL.libsvm")
    
    val cols: List[ColumnHeadTuple] = sparse0.getColumnHeads()
    
    var queue: mutable.Queue[Int] = new mutable.Queue[Int]()
    
    cols.foreach
    {
      cht =>
        if (!isToKeep(cht.stringLabel))
        {
          queue += cht.numericLabel
        }
    }
    
    sparse0.soRemoveColumns(spark, queue)
    sparse0.print(spark, "am", "C:\\Users\\jake\\__workspace\\scalaProjects\\scalaML\\unitTestOutput")
    
    
  }
  
  
  def loadLibSvm(fileName:String): RDDLabeledPoint =
  {
    
    
    loadLibSvm(fileName,"./output")
  }
  
  
  def loadLibSvm(fileName:String, path:String): RDDLabeledPoint =
  {
    
    
    val sparse0: RDDLabeledPoint = new RDDLabeledPoint
    sparse0.setDefaultPartitionSize(3000)
    sparse0.loadLibSvm(spark, fileName, path)
    sparse0
  }
  
  
  
  @Test
  def testSparse(): Unit =
  {
    try
    {
      
      //val sparse0 = loadLibSvm("BY_SENTENCE_ReducedAndJoined","C:\\Users\\jake\\_servers\\spark-2.2.0-bin-hadoop2.7\\output")
       //trimTargets(sparse0)
       // sparse0.sparseOperationEvenProportionPerTarget(spark)
      //  sparse0.printSparse(spark, "BY_SENTENCE_ReducedAndJoinedAndEvened","./output")

      //processKMeansTrimming(sparse0,20)
      
      //
      //mergeRowBlocks()
      //editCsvColumnTest()
      //mergeLibsvm()
      //mergeRowBlocks()
      
      
      
      /////////////////////////////////////////////////////////////
     // trimTargets(sparse0)
      //sparse0.printSparse(spark,"trimmedTarget","./unitTestOutput")
      //sparse0.sparseOperationEvenProportionPerTarget(spark)
      //logger.debug(s"Rows By Target:\n${sparse0.sparseOperationCountRowsByTargetCollected().mkString(" - \n - ")}")
      //logger.debug(s"row count is now ${sparse0.sparseData().count()}")
      
      ///removeNouns(sparse0)
      
      
      ///processDecisionTree(sparse0)
      
      //processLDATrimming(sparse0,25)
      
      
      
     // logger.info("\n\n\n\n\n\n\n\n=======================\nBY_JOB_ReducedAndJoined\n===================\n\n\n\n\n\n\n\n\n\n")
      //val sparse1 = loadLibSvm("BY_JOB_ReducedAndJoined")

      //trimTargets(sparse1)
      //sparse1.sparseOperationEvenProportionPerTarget(spark)
      //processLDATrimming(sparse1,3)
      
      
      //removeRows()
      //    decisionTree()
      
      // reduceAndJoin()
      //loadLibSvmPJ()
      // loadLibSvm
      //assertTrue(true)
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
  
  

  
  
  
  
  @throws
  def removeNouns(sparse0: RDDLabeledPoint): Unit =
  {
    try
    {
      logger.info("\n\nREMOVE NOUNS\n\n")
      
      logger.debug(s"row count    ${sparse0.sparseData().count} ")
      val colsToRemove: List[Int] = sparse0.getColumnHeads().filter(cht => !(cht.stringLabel.contains("V_")) && !(cht.stringLabel.contains("ADJ_"))).map(cht => cht.numericLabel).toList
      
      //logger.debug(colsToRemove.sorted.mkString(" , "))
      
      val q: mutable.Queue[Int] = new mutable.Queue[Int]()
      q ++= colsToRemove
      sparse0.soRemoveColumns(spark, q)
      
      sparse0.printSparse(spark, "nounsstripped", "C:\\Users\\jake\\_servers\\spark-2.2.0-bin-hadoop2.7\\output")
      
      sparse0.clear()
      sparse0.loadLibSvm(spark, "nounsstripped", "C:\\Users\\jake\\_servers\\spark-2.2.0-bin-hadoop2.7\\output")
      logger.debug(s"row count is now   ${sparse0.sparseData().count} ")
      
      
    }
    catch
    {
      case e: Exception => throw new Exception(e)
    }
    
  }
  
  
  def decisionTree(sparse0: RDDLabeledPoint): Unit =
  {
    logger.info("DECISION TREE")
    logger.info(s"target count    ${sparse0.targetCount()} ")
    val sc = spark.sparkContext
    
    
    // Load and parse the data file.
    //val data = MLUtils.loadLibSVMFile(sc, "C:\\Users\\jake\\__workspace\\scalaProjects\\scalaML\\unitTestInput\\BY_JOB_ReducedAndJoined_data_sparse\\part-00000")
    val data: RDD[LabeledPoint] = sparse0.lpData()
    logger.debug(s"split")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(1, 9))
    val (trainingData, testData) = (splits(0), splits(1))
    
    val numClasses = sparse0.targetCount() + 1
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 15
    val maxBins = 32
    
    logger.debug(s"create model")
    
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
    
    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map
    { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map
    { case (v, p) => math.pow(v - p, 2) }.mean()
    logger.info("Test Mean Squared Error = " + testMSE)
    logger.info("Learned regression tree model:\n" + model.toDebugString)
    logger.debug(s"save")
    
    // Save and load model
    DataFrameUtils.deletePartition("target/tmp/myDecisionTreeRegressionModel")
    //model.save(sc, "target/tmp/myDecisionTreeRegressionModel")
    //val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeRegressionModel")
    
    
    logger.info(subtreeToString(sparse0, model.topNode, 0))
    
    
    logger.debug(getTree(sparse0, new Tuple2(model.topNode.leftNode, model.topNode.rightNode)))
    logger.debug(wordsMap.mkString("\n"))
    logger.debug(s"words  collected: ${wordsMap.size}")
  }
  
  
  var nodeCount: Int = 0
  
  @throws
  def processDecisionTree(sparse0: RDDLabeledPoint): Unit =
  {
    try
    {
      wordsMap.clear()
      decisionTree(sparse0)
      logger.debug(s"wordcount ${wordsMap.size}")
      
      
    }
    catch
    {
      case e: Exception => throw new Exception(e.getMessage, e)
    }
  }
  
  
  def processNode(sparse0: RDDLabeledPoint, node: Option[Node], level: Int, right: Boolean, wordsMap: mutable.HashMap[String, Int]): String =
  {
    if (node.isDefined)
    {
      val n: Node = node.get
      var term: String = ""
      val prefix = "\t" * level
      var threshold: String = ""
      
      if (n.split.isDefined)
      {
        val split = n.split.get
        term = (sparse0.getColumnName(split.feature))
        
        
        if (!wordsMap.get(term).isDefined)
        {
          wordsMap.put(term, level)
        }
        threshold = split.threshold.toString
      }
      nodeCount = nodeCount + 1
      if (right)
      {
        s"$prefix Node #$nodeCount Level: $level IS $term: ${sparse0.getTargetName(n.predict.predict)} > $threshold \n $prefix ${n.toString()}"
      }
      else if (term.isEmpty)
           {
             s"$prefix Node #$nodeCount Level: $level ${sparse0.getTargetName(n.predict.predict)}\n $prefix ${n.toString()}"
           }
      else
      {
        s"$prefix Node #$nodeCount Level: $level NOT $term < ${threshold})\n"
      }
      
      
    }
    else
    {
      ""
    }
  }
  
  
  private def subtreeToString(sparse0: RDDLabeledPoint, n: Node, indentFactor: Int = 0): String =
  {
    
    def splitToString(split: org.apache.spark.mllib.tree.model.Split, left: Boolean): String =
    {
      split.featureType match
      {
        case Continuous => if (left)
                           {
                             s"(feature ${sparse0.getColumnName(split.feature)} <= ${split.threshold})"
                           }
                           else
                           {
                             s"(feature ${sparse0.getColumnName(split.feature)}) > ${split.threshold})**"
                           }
        case Categorical => if (left)
                            {
                              s"(feature ${sparse0.getColumnName(split.feature)} in ${split.categories.mkString("{", ",", "}")})"
                            }
                            else
                            {
                              s"(feature ${sparse0.getColumnName(split.feature)} not in ${split.categories.mkString("{", ",", "}")})"
                            }
      }
    }
    
    val prefix: String = " " * indentFactor
    val prefixR: String = "*" * indentFactor
    if (n.isLeaf)
    {
      prefix + s"Predict: ${sparse0.getTargetName(n.predict.predict)}\n"
    }
    else
    {
      if (n.split.isDefined)
      {
        "\n" + prefix + s"If ${splitToString(n.split.get, left = true)}\n" +
          subtreeToString(sparse0, n.leftNode.get, indentFactor + 1) +
          prefixR + s"Else ${splitToString(n.split.get, left = false)}\n" +
          subtreeToString(sparse0, n.rightNode.get, indentFactor + 1)
      }
      else
      {
        ""
      }
    }
  }
  
  @throws
  def getTree(sparse0: RDDLabeledPoint, nodeTuple: Tuple2[Option[Node], Option[Node]], inlevel: Int = 0): String =
  {
    
    
    var level = inlevel + 1
    val sb: StringBuilder = new mutable.StringBuilder()

    sb.append("\n")
    if (nodeTuple._1.isDefined)
    {
      sb.append(processNode(sparse0, nodeTuple._1, level, false, wordsMap))
      if (!(nodeTuple._1.get.isLeaf))
      {
        sb.append(getTree(sparse0, new Tuple2(nodeTuple._1.get.leftNode, nodeTuple._1.get.rightNode), level))
      }
      
      if (nodeTuple._2.isDefined)
      {
        sb.append(processNode(sparse0, nodeTuple._2, level, true, wordsMap))
        if (!(nodeTuple._2.get.isLeaf))
        {
          sb.append(getTree(sparse0, new Tuple2(nodeTuple._2.get.leftNode, nodeTuple._2.get.rightNode), level))
        }
      }
    }
    sb.toString()
  }
  
  
  @throws
  def doKmeans(sparse0:RDDLabeledPoint, numClusters:Int=10, proportion:Double=0.9): KMeansModel =
  {
    try
    {
      // Load and parse the data
      val splits = sparse0.lpData().randomSplit(Array(proportion, 1-proportion))
      val (trainingData, testData) = (splits(0), splits(1))
  
      val mappedData: Seq[org.apache.spark.mllib.linalg.Vector] = trainingData.collect().map
      {
       
        row =>
          row.features.toSparse
      }.toSeq
  
  
      val mappedRDD = spark.sparkContext.parallelize(mappedData)
  
      // Cluster the data into two classes using KMeans
      
      val numIterations = 20
 
      val clusters=KMeans.train(mappedRDD, numClusters, numIterations)

      val WSSSE = clusters.computeCost(mappedRDD)
      logger.info("Within Set Sum of Squared Errors = " + WSSSE)
      clusters

    }
    catch
      {
        case e:Exception =>
          logger.error(e.getMessage,e)
          throw new Exception(e)
      }
  }
  
  @throws
  def doLDA(data: RDD[LabeledPoint], lda_topicCount: Int = 20, proportion:Double = 0.9): LDAModel =
  {
    
    if(proportion > 1)
      {
        throw new Exception("Proportion must be a value less than  1")
      }
    // Load and parse the data
    //val data = spark.sparkContext.textFile("data/mllib/sample_lda_data.txt")
    //val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    // Index documents with unique IDs
    
    //val parsedData = spark.read.format("libsvm").load("data/mllib/sample_lda_libsvm_data.txt")
    try
    {
      
      
      logger.info("DO LDA");
      
      
      
      val splits = data.randomSplit(Array(proportion, 1-proportion))
      val (trainingData, testData) = (splits(0), splits(1))
      
      val mappedData: Map[Long, org.apache.spark.mllib.linalg.Vector] = trainingData.collect().map
      {
        var count: Long = 0
        row =>
          count = count + 1
          
          new Tuple2(count, row.features.toSparse)
      }.toMap
      
      
      val mappedRDD = spark.sparkContext.parallelize(mappedData.toSeq)
      
      
      logger.info("Running Model")
      // Cluster the documents into three topics using LDA
      
      
      val lda = new LDA()
      lda.setOptimizer("online")
      lda.setK(lda_topicCount).run(mappedRDD)
    }
    catch
    {
      case e: Exception => throw new Exception(e)
    }
  }
  
  
  
  
  def reportKmeans(kModel:KMeansModel, sparse0:RDDLabeledPoint):  mutable.HashMap[Int,Int] =
  {

    
    ///logger.info(s"k is ${ldaModel.k}")
    
    val termToTopicCount:mutable.HashMap[Int,Int]=new mutable.HashMap()
  
  
    val sb:StringBuilder=new StringBuilder
    kModel.clusterCenters.toSeq.foreach
    {
    
      v =>
      
        sb.append("\n\n-------------\n\n")
      
        v.toSparse.indices.foreach
        {
          f =>
            if(v(f)>0.13)
            {
              sb.append(sparse0.getColumnName(f))
              sb.append(" - ")
              sb.append(v(f))
              sb.append("\n")
              val clusterinclusioncountO = termToTopicCount.get(f)

              if (!clusterinclusioncountO.isDefined)
              {
                termToTopicCount.put(f, 0)
              }
              else
              {
                termToTopicCount.put(f, (clusterinclusioncountO.get + 1))
              }
            }
        }
    }
    logger.debug(sb)
    termToTopicCount
    
  }
  
  
  def reportLDA(ldaModel:LDAModel, sparse0:RDDLabeledPoint):  mutable.HashMap[Int,Int] =
  {
    // Output topics. Each is a distribution over words (matching word count vectors)
    logger.info("\n\nLearned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):\n")
  
    ///logger.info(s"k is ${ldaModel.k}")
  
    val termToTopicCount:mutable.HashMap[Int,Int]=new mutable.HashMap()
    val topics = ldaModel.topicsMatrix
  
    val sb: StringBuilder = new mutable.StringBuilder()
  
  
    val topicDescriptions: Array[(Array[Int], Array[Double])] = ldaModel.describeTopics(maxTermsPerTopic = 60)
  
    logger.info(s"topicDescriptions ${topicDescriptions.size}")
  
    //logger.debug(topicDescriptions)
  
  
    topicDescriptions.foreach
    {
    
      case (terms, termWeights) =>
      
        terms.zip(termWeights).foreach
        {
          case (term, weight) =>
            if (weight > 0.002)
            {
              sb.append((f"${sparse0.getColumnName(term.toInt)} [$weight%1.2f]   \t"))
              val topicinclusioncountO = termToTopicCount.get(term.toInt)
            
            
              if (!topicinclusioncountO.isDefined)
              {
                termToTopicCount.put(term.toInt, 0)
              }
              else
              {
                termToTopicCount.put(term.toInt, (topicinclusioncountO.get + 1))
              }
            }
        
        }
      
        sb.append("\n\n")
    }
    sb.append("\n\n")
  
  
    logger.debug(sb)
    /** val colsToRemove: List[Int] = iterativeTrimming(termToTopicCount, lda_topicCount)
      * *
      * val q: mutable.Queue[Int] = new mutable.Queue[Int]()
      * q ++= colsToRemove
      * *
      *logger.info(s"\n\nRemoving ${colsToRemove.size} columns\n\n")
      * *
      * if (colsToRemove.size > 0)
      * {
      * sparse0.sparseOperationRemoveColumns(spark, q)
      * processLDATrimming(sparse0)
      * } */
  
    termToTopicCount
  
  }
  
  
  def doLDACycle(lda_topicCount:Int, sparse0:RDDLabeledPoint): Unit =
  {

    logger.info("\n\t\tLDA CYCLE\n")
    val ldaModel: LDAModel = doLDA(sparse0.lpData(), lda_topicCount)
    val termToTopicCount:mutable.HashMap[Int,Int]=reportLDA(ldaModel,sparse0)
    val colsToRemove:List[Int]=iterativeTrimmingForLDA(termToTopicCount, lda_topicCount,sparse0)
    if(colsToRemove.size>2)
    {
      var q: mutable.Queue[Int] = new mutable.Queue[Int]()
      q ++= colsToRemove
      sparse0.soRemoveColumns(spark, q)
      doLDACycle(lda_topicCount,sparse0)
    }
  }
  
  def doKmeansCycle(km_clusterCount:Int, sparse0:RDDLabeledPoint): Unit =
  {
    
    logger.info("\n\t\tKmeans CYCLE\n")
    val kModel: KMeansModel = doKmeans(sparse0, numClusters = km_clusterCount, proportion=0.9)
  

    
    
    
    val termToTopicCount:mutable.HashMap[Int,Int]=reportKmeans(kModel,sparse0)
    termsToKeep ++=termToTopicCount.keySet
    val colsToRemove:List[Int]=iterativeTrimmingForLDA(termToTopicCount, km_clusterCount,sparse0)
    if(colsToRemove.size>0)
    {
      var q: mutable.Queue[Int] = new mutable.Queue[Int]()
      q ++= colsToRemove
      sparse0.soRemoveColumns(spark, q)
      doKmeansCycle(km_clusterCount,sparse0)
    }
  }
  
  
  def processLDATrimming(sparse0: RDDLabeledPoint, lda_topicCount:Int): Unit =
  {
    try
    {
      
      logger.info(("\n\n\nPROCESS LDA TRIMMING\n\n\n"))
      doLDACycle(lda_topicCount,sparse0.copy(spark))

      val targets: Map[String, Double] = sparse0.getTargets()
      targets.foreach
      {
        case (stringValue, numericValue) =>
        {
          logger.info(s"\n\n\n\nNEW TARGET $stringValue\n\n\n")
          logger.info(s"")
          val sparseSubsetO:Option[RDDLabeledPoint]=sparse0.soSubsetByTarget(spark,numericValue,stringValue)
          if(sparseSubsetO.isDefined)
          {
            doLDACycle(lda_topicCount, sparseSubsetO.get)
          }
        }
      }
    }
    catch
    {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new Exception(e)
    }
  }
  
  def columnsNotInList(sparse0:RDDLabeledPoint): Seq[Int] =
  {
    val columnHeads:List[ColumnHeadTuple]=sparse0.getColumnHeads()
    columnHeads.filter(cht => !termsToKeep.contains(cht.numericLabel)).map(cht => cht.numericLabel)
    
  }
  
  
  def processKMeansTrimming(sparse0: RDDLabeledPoint, km_clusterCount:Int): Unit =
  {
    try
    {
      
      logger.info(("\n\n\nPROCESS KMeans TRIMMING\n\n\n"))
      doKmeansCycle(km_clusterCount,sparse0.copy(spark))
      
      val targets: Map[String, Double] = sparse0.getTargets()
      targets.foreach
      {
        case (stringValue, numericValue) =>
        {
          logger.info(s"\n\n\n\nNEW TARGET $stringValue\n\n\n")
          logger.info(s"")
  
          val sparseSubsetO:Option[RDDLabeledPoint]=sparse0.soSubsetByTarget(spark,numericValue,stringValue)
          if(sparseSubsetO.isDefined)
          {
            doKmeansCycle(km_clusterCount/2, sparseSubsetO.get)
          }

        }
      }



      var queue: mutable.Queue[Int] = new mutable.Queue[Int]()
      queue ++= columnsNotInList(sparse0)
      sparse0.soRemoveColumns(spark,queue)
      sparse0.printSparse(spark,"BY_SENTENCE_ReducedAndJoinedAndEvenedTrimmedByKmeans","./output")
      
      
      
    }
    catch
    {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new Exception(e)
    }
  }
  
  
  def iterativeTrimmingForLDA(termToTopicCount: mutable.HashMap[Int, Int], lda_topicCount: Int, sparse0:RDDLabeledPoint): List[Int] =
  {
    ///logger.debug(s"termToTopicCount ${termToTopicCount.mkString("\n")}")
    termToTopicCount.map
    {
      tuple =>
        
        if (tuple._2 > (lda_topicCount / 3))
        {
          logger.info(s"removing ${sparse0.getColumnName(tuple._1)}")
          tuple._1.toInt
        }
    }.toList collect
      { case i: Int => i }
  }
  
  
  def iterativeTrimmingForDecisionTree(sparse0: RDDLabeledPoint): List[Int] =
  {
    
    logger.debug(s"wordsMap ${wordsMap.mkString("\n")}")
    wordsMap.map
    {
      tuple =>
        if (tuple._2 < 5)
        {
          sparse0.getColumnIndex(tuple._1)
        }
      
    }.toList collect
      { case i: Int => i }
    
    
  }


  **********/
}
