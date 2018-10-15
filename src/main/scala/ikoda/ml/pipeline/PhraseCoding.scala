package ikoda.ml.pipeline

import java.io.File

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.SparkDirectoryFinder
import ikoda.ml.caseclasses.CodedSentence
import ikoda.ml.cassandra.{QueryExecutor, SparseSupplementDataInput, SparseSupplementModelMaker}
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.{DataFrameUtils, SparkConfProviderWithStreaming, UtilFunctions}
import ikoda.utils.{IDGenerator, Spreadsheet}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PhraseCoding(pconfig: PipelineConfiguration) extends Logging  with UtilFunctions with QueryExecutor
{

  private def clearInputData(): Unit =
  {
    Spreadsheet.getInstance().getCsvSpreadSheet(pconfig.get(PipelineConfiguration.phraseCodingTrainingDataFile)).clearAll()
  }

  @throws(classOf[IKodaMLException])
   def phraseCodingSaveDataToCassandra(sparse0: Option[RDDLabeledPoint]): Option[RDDLabeledPoint] =
  {
    try
    {
      val codedSentences: scala.collection.mutable.Queue[CodedSentence] = mapCodeRows(openCodeFile(pconfig))
      clearInputData
      val cleanCsvPatho=convertCodesToCleanCsv(codedSentences)
      if(cleanCsvPatho.isDefined)
      {
        saveToCassandra(cleanCsvPatho.get)
      }



      sparse0

    }
    catch
    {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def phraseCodingModelGeneration(sparse0: RDDLabeledPoint): RDDLabeledPoint =
  {
    try
    {
      val codedSentenceso: Option[scala.collection.mutable.Queue[CodedSentence]] = None


      if(codedSentenceso.isDefined)
      {
        generateTrainingSet(codedSentenceso.get)
        logger.info("Convert to RDDLabeledPoint")
        val sparse0: RDDLabeledPoint = new RDDLabeledPoint

        showMemoryUsage
        sparse0.transformLibSvmProcessorToRDDLabeledPoint(getSparkSession(),
          Spreadsheet.getInstance().getLibSvmProcessor(pconfig.get(PipelineConfiguration.phraseCodingTrainingDataFile)))

        RDDLabeledPoint.printSparseLocally(sparse0, pconfig.get(PipelineConfiguration.phraseCodingTrainingDataFile),
          pconfig.get(PipelineConfiguration.phraseAnalysisDataSourceRootPath))
        generateModel(sparse0)

      }
      sparse0

    }
    catch
    {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }


  private def saveToCassandra(csvPath:String): Unit =
  {
    try
    {
      val ssmm:SparseSupplementModelMaker=new SparseSupplementModelMaker(pconfig)
      ssmm.createTablesIfNotExist(csvPath,"competencycodes")
      val ssdi:SparseSupplementDataInput=new SparseSupplementDataInput(pconfig)
      ssdi.insertSupplementaryData(csvPath,"competencycodes",false,true)


    }
    catch
      {
        case e:Exception => logger.error(e.getMessage,e)
      }

  }


  private def convertCodesToCleanCsv(codedSentences:Seq[CodedSentence]): Option[String] =
  {
    try
    {
      val csvName=pconfig.get(PipelineConfiguration.phraseCodingCleanOutputFile)
      Spreadsheet.getInstance().initCsvSpreadsheet(csvName,"ikoda",pconfig.get(PipelineConfiguration.pipelineOutputRoot))


      codedSentences.foreach
      {
        cs => Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(cs.A_RowId,"uuid",cs.A_RowId)
          Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(cs.A_RowId,"a_uid",cs.A_RowId)
              Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(cs.A_RowId,"aa_label",cs.labelId.toString)
              Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(cs.A_RowId,"bagofwords",cs.lemmatizedPhrase.mkString(", "))
              Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(cs.A_RowId,"aa_labelName",cs.labelName)

             // val ss:collection.immutable.SortedSet[String] = collection.immutable.SortedSet[String]() ++ cs.codes
              cs.codes.foreach
              {
                code=>Spreadsheet.getInstance().getCsvSpreadSheet(csvName).addCell(cs.A_RowId, cleanColumnName(code), "1")

              }

      }
      Spreadsheet.getInstance().getCsvSpreadSheet(csvName).printCsvFinal()
      Option(Spreadsheet.getInstance().getCsvSpreadSheet(csvName).getPathToDirectory+File.separator+"FINAL_"+pconfig.get(PipelineConfiguration.phraseCodingCleanOutputFile))

    }
    catch
      {
        case e:Exception=> logger.error(e.getMessage,e)
          None
      }
  }


  @throws(classOf[IKodaMLException])
  private def generateModel(sparse0: RDDLabeledPoint): Unit =
  {
    try
    {
      sparse0.getTargets().foreach
      {
        case (name,idx) =>
          val sparse1o:Option[RDDLabeledPoint]=RDDLabeledPoint.dichotomizeTargetOrOther(sparse0,idx,name)
          if(sparse1o.isDefined)
            {
              val sparse= sparse1o.get
              try
              {

                val sparseEvened=getOrThrow(RDDLabeledPoint.evenProportionPerTarget(sparse))
                val numIterations = 100
                val model = SVMWithSGD.train(sparseEvened.convertToMLLibPackageRDD(), numIterations)
                DataFrameUtils.deletePartition(
                  s"${pconfig.get(PipelineConfiguration.phraseCodingModelRootDir)}${File.separator}SVM-CodedPhrases-$name"
                )
                model.save(
                  getSparkSession().sparkContext,
                  s"${pconfig.get(PipelineConfiguration.phraseCodingModelRootDir)}${File.separator}SVM-CodedPhrases-$name"
                )
                RDDLabeledPoint.printSparseLocally(sparseEvened,
                   s"Schema_${name}", pconfig.get(PipelineConfiguration.phraseCodingModelRootDir)
                )
              }
              catch
                {
                  case ex:Exception =>
                    logger.error(RDDLabeledPoint.diagnostics(sparse))
                    logger.error(ex.getMessage,ex)

                }

            }

      }

    }
    catch
    {
      case e: Exception =>
        logger.error(RDDLabeledPoint.diagnostics(sparse0))
        logger.error(e.getMessage, e)



    }
  }

  @throws(classOf[IKodaMLException])
  private def loadSchemaForCode(currentModel:String,fileList:List[org.apache.hadoop.fs.Path]): Option[RDDLabeledPoint] =
  {
    try
      {
        logger.warn("\n\n\n\n\nMethod stub\n\n\n\n\n")
        None

      }

    catch
      {
        case e:Exception => throw IKodaMLException(e.getMessage,e)
      }

  }
  @throws(classOf[IKodaMLException])
  def phraseCodePrediction(sparse0: RDDLabeledPoint): Unit =
  {
    try
    {
      logger.info("phraseCodePrediction")
      val modelDirFiles:List[org.apache.hadoop.fs.Path]=SparkDirectoryFinder.directoryStructure(pconfig.get(PipelineConfiguration.phraseCodingModelRootDir)).toList
      val subseto=RDDLabeledPoint.randomSubset(sparse0,0.01)
      modelDirFiles.foreach
      {
        file =>
          if (file.toString.contains("SVM-CodedPhrases-"))
          {
            logger.info(s"\n\nLoading model $file\n\n")
            val svmModel = SVMModel.load(getSparkSession().sparkContext, file.toString)
            val sparseSchemao=loadSchemaForCode(file.toString,modelDirFiles)
            if(sparseSchemao.isDefined)
            {
              logger.info("Loaded schema and model")
              val sparseSchema:RDDLabeledPoint=sparseSchemao.get






              if(subseto.isDefined)
              {
                val sparseConvertedo=sparseSchema.transformToRDDLabeledPointWithSchemaMatchingThis(subseto.get)
                if(sparseConvertedo.isDefined)
                {
                  val results:Seq[(String,String)]=doPrediction(svmModel,sparseConvertedo.get )
                  saveResults(results)
                }
              }
            }
            else
            {
              throw IKodaMLException(s"No Schema found for $file")
            }
          }
      }

    }
    catch
    {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  @throws
  private def saveResults(resultSeq:Seq[(String, String)]): Unit =
  {
    try
    {
      Spreadsheet.getInstance().initCsvSpreadsheet("output11","ikoda",pconfig.get(PipelineConfiguration.phraseAnalysisDataSourceRootPath))
      resultSeq.foreach
      {
        res=>
          val uid=String.valueOf(IDGenerator.getInstance().nextIDInMem())
          Spreadsheet.getInstance().getCsvSpreadSheet("output11").addCell(uid,"qq",res._1)
          Spreadsheet.getInstance().getCsvSpreadSheet("output11").addCell(uid,"dd",res._2)

      }
      Spreadsheet.getInstance().getCsvSpreadSheet("output11").printCsvFinal()
    }
    catch
      {
        case e:Exception => throw IKodaMLException(e.getMessage,e)
      }
  }

  @throws
  private def doPrediction(svmModel:SVMModel, sparse0:RDDLabeledPoint): Seq[(String,String)] =
  {
    try
    {
      sparse0.convertToMLLibPackageRDD().collect.map
      {
        lp =>
          val targetIdx = svmModel.predict(lp.features)
          val targetString = sparse0.getTargetName(targetIdx)
          val phrase = concatenateColumns(sparse0, lp)
          (targetString, phrase)

      }

    }
    catch
      {
        case e:Exception => throw new IKodaMLException(e.getMessage,e)
      }
  }

  def concatenateColumns(schema:RDDLabeledPoint, lp:LabeledPoint): String =
  {
    lp.features.toSparse.indices.map
    {
      idx => schema.getColumnName(idx)
    }.mkString(" ")
  }


  @throws(classOf[IKodaMLException])
  private def generateTrainingSet(codedSentences: scala.collection.mutable.Queue[CodedSentence]): Unit =
  {
    try
    {
      logger.info("generateTrainingSet")
        val tdFileName=pconfig.get(PipelineConfiguration.phraseCodingTrainingDataFile)
        val targetCol="Target"
        val majorCol="MajorCode"
      Spreadsheet.getInstance().initLibsvm2(tdFileName,"ikoda",targetCol,pconfig.get(PipelineConfiguration.phraseAnalysisDataSourceRootPath))
      while (codedSentences.length>0)
      {
        val cs:CodedSentence= codedSentences.dequeue
          cs.codes.foreach
          {
            code =>
              val uid:String=String.valueOf(IDGenerator.getInstance().nextIDInMem())
              Spreadsheet.getInstance().getLibSvmProcessor(tdFileName).addCell(
                uid, majorCol, String.valueOf(cs.labelId)
              )
              Spreadsheet.getInstance().getLibSvmProcessor(tdFileName).addCell(
                uid, targetCol, code)
              val tempSeq:Seq[String]= mutable.Seq(cs.lemmatizedPhrase:_*)

              tempSeq.foreach
              {
                term => Spreadsheet.getInstance().getLibSvmProcessor(tdFileName).addCell(uid, term, "1")
              }

          }


      }

    }
    catch
    {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  @throws(classOf[IKodaMLException])
  private def openCodeFile(pconfig: PipelineConfiguration): mutable.Map[String, java.util.HashMap[String, String]] =
  {
    try
    {

      logger.info("openCodeFile")
      Spreadsheet.getInstance().initCsvSpreadsheet(
        pconfig.get(PipelineConfiguration.phraseCodingTrainingDataFile),
        pconfig.get(PipelineConfiguration.phraseAnalysisDataSourceRootPath)
      )
      Spreadsheet.getInstance().getCsvSpreadSheet(pconfig.get(PipelineConfiguration.phraseCodingTrainingDataFile)).loadCsv(
        pconfig.get(PipelineConfiguration.phraseCodingTrainingDataFile),
        pconfig.get(PipelineConfiguration.phraseAnalysisInputCsvUidColumnName)
      )
      val data = Spreadsheet.getInstance().getCsvSpreadSheet(
        pconfig.get(PipelineConfiguration.phraseCodingTrainingDataFile)
      ).getData.asScala

      logger.info(s"Loaded ${data.size} rows")

      data
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }

  @throws(classOf[IKodaMLException])
  private  def mapCodeRows(data: mutable.Map[String, java.util.HashMap[String, String]]): scala.collection.mutable.Queue[CodedSentence] =
  {
    try
    {
      logger.info("mapCodeRows")
      val lemmatizedPhraseCol = "lemmatizedPhrase"
      val codeCol = "CODE"
      val targetIdCol = "targetId"
      val targetNameCol = "targetName"
      val tempSeq:Seq[CodedSentence]=data.map
      {
        r =>
          val rowData: mutable.Map[String, String] = r._2.asScala

          val rowId: String = r._1
          val ab: ArrayBuffer[String] = new ArrayBuffer[String]()
          var lemmaSeq: Seq[String] = Seq()
          var targetId: Int = 0
          var targetName=""

          rowData.foreach
          {
            cell =>
              cell._1 match
              {
                case `lemmatizedPhraseCol` => lemmaSeq = cell._2.split(" ")
                case x if (x.toUpperCase().contains(codeCol)) => ab += cell._2
                case x if (x.toUpperCase().contains(targetIdCol.toUpperCase())) => targetId = cell._2.toInt
                case x if (x.toUpperCase().contains(targetNameCol.toUpperCase)) => targetName = cell._2.toString
                case _ => ()

              }

          }
          if(lemmaSeq.isEmpty)
            {
              throw new IKodaMLException("No terms found in entry: targetId= "+targetId+" codes="+ab.mkString(","))
            }
          new CodedSentence(rowId,targetId, targetName,lemmaSeq, ab.toSet)
      }.toSeq
      scala.collection.mutable.Queue(tempSeq : _*)
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


}
