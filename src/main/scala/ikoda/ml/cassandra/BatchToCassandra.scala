package ikoda.ml.cassandra

import java.io.File

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.SparkDirectoryFinder
import ikoda.ml.cassandra.BatchToCassandra.persistFailedPath
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.{SparkConfProviderWithStreaming, UtilFunctions}
import ikoda.utils.TicToc

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * BatchToCassandra is the coordinating object for persisting data to Cassandra
  *
  * It's primary functions are to:
  *
  * - check configuration file for keyspaces to monitor. These settings are loaded through [[CassandraKeyspaceConfigurationFactory]]
  *
  * - monitor pre-specified Hadoop staging directories at a fixed interval (also loaded through [[CassandraKeyspaceConfigurationFactory]])
  *
  * - open files arriving in staging directories and process either as sparse data or as supplementary dense data
  *
  * - if configured to do so, link supplementary and sparse data with a UUID
  *
  * - persist data to Cassandra
  *
  * - deletes corrupted batches if persisted to staging keyspace (but, note, this creates tombstones)
  *
  * - if configured to do so, call [[BatchToCassandraFlushStaging]] at a specified row count threshold. This will remove very low frequency columns, validate data, and flush data from staging to a mirror permanent keyspace.
  *
  * - moves Hadoop files from staging directory to either persisted or failed directories depending on data processing outcome.
  */
object BatchToCassandra extends Logging    with UtilFunctions with QueryExecutor with BatchToCassandraTrait
{
  private var continueRun = true;
  private var threadStarted = false
  private var currentCycleRunning=false

  @throws(classOf[IKodaMLException])
  private def batchesForCassandra(): List[org.apache.hadoop.fs.Path] =
  {
    val buffer: ArrayBuffer[org.apache.hadoop.fs.Path] = new ArrayBuffer[org.apache.hadoop.fs.Path]()
    SparkDirectoryFinder.cassandraBatchDirectoriesSparse().foreach
    {
      path =>
        val batch:Seq[org.apache.hadoop.fs.Path] = SparkDirectoryFinder.pathsOlderThan((System.currentTimeMillis() - filesOlderThan), new org.apache.hadoop.fs.Path(path))
          .filter(p=> !p.toString.toLowerCase().contains("target"))
          .filter(p => !p.toString.toLowerCase().contains("target"))
          .filter(p => !p.toString.toLowerCase().contains("column"))
          .filter(p => !p.toString.toLowerCase().contains("archive"))
          .filter(p => !p.toString.toLowerCase().contains("persisted"))
          .filter(p => !p.toString.toLowerCase().contains("failed"))
        buffer ++= batch
    }

    logger.info("Available Batches: " + buffer.mkString("\n"))
    buffer.toList
  }


  @throws(classOf[IKodaMLException])
  private def cleanInboxDirectory(): Unit =
  {

    try {
      val buffer: ArrayBuffer[org.apache.hadoop.fs.Path] = new ArrayBuffer[org.apache.hadoop.fs.Path]()
      SparkDirectoryFinder.cassandraBatchDirectoriesSparse().foreach {
        path =>
          val batch: Seq[org.apache.hadoop.fs.Path] = SparkDirectoryFinder.pathsOlderThan((System.currentTimeMillis() - 10000000), new org.apache.hadoop.fs.Path(path))
            .filter(p => !p.toString.toLowerCase().contains("target"))
            .filter(p => !p.toString.toLowerCase().contains("target"))
            .filter(p => !p.toString.toLowerCase().contains("column"))
            .filter(p => !p.toString.toLowerCase().contains("archive"))
            .filter(p => !p.toString.toLowerCase().contains("persisted"))
            .filter(p => !p.toString.toLowerCase().contains("failed"))
          buffer ++= batch
      }

      logger.info("Available Batches: " + buffer.mkString("\n"))
      buffer.foreach {
        failedFile =>
          val kso = keySpaceFromPath(failedFile.toString)
          logger.warn("Homeless straggler file: " + failedFile)
          val outpath = kso.isDefined match {
            case true => CassandraKeyspaceConfigurationFactory.keyspaceConfig(kso.get).errordir
            case false => Try(CassandraKeyspaceConfigurationFactory.keyspaceConfig("defaultks").errordir).isSuccess match {
              case true => CassandraKeyspaceConfigurationFactory.keyspaceConfig("defaultks").errordir
              case false => "/ikoda/lostandhomeless"
            }
          }
          SparkDirectoryFinder.renamePartition(failedFile.toString, outpath + File.separator + failedFile.getName)
      }
    }
    catch
      {
        case e:Exception => logger.error(e.getMessage,e)
      }
  }





  @throws(classOf[IKodaMLException])
  private def batchSupplementsForCassandra(): List[org.apache.hadoop.fs.Path] =
  {
    val buffer: ArrayBuffer[org.apache.hadoop.fs.Path] = new ArrayBuffer[org.apache.hadoop.fs.Path]()
    SparkDirectoryFinder.cassandraBatchSupplementDirectoriesSparse().foreach
    {
      path =>
        val batch = SparkDirectoryFinder.pathsOlderThan(System.currentTimeMillis() - (filesOlderThan+20000), new org.apache.hadoop.fs.Path(path))
          .filter(p => !p.toString.contains("target"))
          .filter(p => !p.toString.contains("column"))
          .filter(p => !p.toString.contains("archive"))
          .filter(p => !p.toString.contains("persisted"))
          .filter(p => !p.toString.contains("persistFailed"))
        buffer ++= batch
    }

    logger.info("Available Supplements: " + buffer.mkString("\n"))
    buffer.toList
  }


  private def callFlush(): Unit =
  {
    try
    {
      CassandraKeyspaceConfigurationFactory.keySpaceNames().foreach
      {
        ks=>
        val ckc=CassandraKeyspaceConfigurationFactory.keyspaceConfig(ks)
        flushStagingKeySpace(ks, ckc.flushthreshold)
      }
    }
    catch
      {
        case e:Exception => logger.error("Handled "+e.getMessage,e)
      }
  }

  private [cassandra] def doBatch(): Unit =
  {
    try
    {
      val tt: TicToc = new TicToc()
      val batches: List[org.apache.hadoop.fs.Path] = batchesForCassandra()
      if (!batches.isEmpty)
      {
        batches.foreach
        {
          batch =>
            val keySpaceNameo: Option[String] = keySpaceFromPath(batch.toString)
            val keySpaceUuido: Option[String] = keySpaceUuidFromPath(batch.toString)
            if (keySpaceNameo.isDefined && keySpaceUuido.isDefined && continueRun)
            {

              registerKeyspaceForSparse(keySpaceNameo.get)
              logger.info(tt.tic("\n\n--------------------\nSave batch " + batch + "\n--------------------\n", 60000))
              saveBatch(batch, keySpaceNameo.get, keySpaceUuido.get)

              logger.info(tt.toc("\n\n--------------------\nSave batch " + batch + "\n--------------------\n"))
            }
        }


      }
    }
    catch
      {
        case e:Exception => throw new IKodaMLException(e.getMessage,e)
      }
  }



  private def doBatchSupplement(): Unit =
  {
    try {
      val tt: TicToc = new TicToc()
      if (continueRun) {
        val batchSupplements: List[org.apache.hadoop.fs.Path] = batchSupplementsForCassandra()
        if (!batchSupplements.isEmpty) {
          batchSupplements.foreach {
            bs =>
              logger.info(tt.tic("\n\n--------------------\nSave supplement " + bs + "\n--------------------\n", 30000))
              saveBatchSupplement(bs)
              logger.info(tt.toc("\n\n--------------------\nSave supplement " + bs + "\n--------------------\n"))
          }
        }
      }
    }
    catch
      {
        case e:Exception => throw new IKodaMLException(e.getMessage,e)
      }
  }


  private def elegantExit():Boolean=
  {
    try {
      val myCfg = ConfigFactory.parseFile(new File("./ikoda/conf/b2crun.conf"))

      myCfg.getBoolean("elegantStop.stop")
    }
    catch
      {
        case e:Exception => logger.error(e.getMessage,e)
          false
      }
  }

  /**
    * Starts the BatchToCassandra process
    *
    */
  @throws(classOf[IKodaMLException])
  def monitorSparkDirectories(): Unit =
  {
    logger.info("Starting monitorSparkDirectories threadStarted="+threadStarted)

    try {
      if (!threadStarted) {



        val thread = new Thread {
          override def run {
            threadStarted = true
            continueRun = true
            logger.debug(threadStarted + " " + continueRun)

            logger.info("Logger Name: " + logger.name)
            while (continueRun) {




              currentCycleRunning = true
              val tt: TicToc = new TicToc()
              logger.info(tt.tic("\n\n--------------------\nMonitoring Staging Keyspaces\n--------------------\n", 180000))
              doBatch()
              doBatchSupplement()
              callFlush


              BatchToCassandraFlushStaging.runFlushMonitor

              cleanInboxDirectory
              logger.info(tt.toc("\n\n--------------------\nMonitoring Staging Keyspaces\n--------------------\n"))
              currentCycleRunning = false
              if(elegantExit())
                {
                  stopB2c()
                }
              Thread.sleep(sleepTime)

            }

            logger.info("\n==================\nB2C Exited \n==================\n")

          }

          threadStarted = false

        }
        thread.start
      }




    }
    catch
      {
        case e:Exception =>
          threadStarted=false
          currentCycleRunning=false
          continueRun=false
          logger.error("\n\nBatchToCassandra Thread Died\n"+e.getMessage+"\n",e)
      }
  }

  private [cassandra] def createNewKeyspace(keyspaceName: String): String =
  {
    try
    {
      val pconfig: PipelineConfiguration = new PipelineConfiguration
      pconfig.config(PipelineConfiguration.keyspaceName, keyspaceName)
      val smm: SparseModelMaker = new SparseModelMaker(pconfig)
      smm.createNewKeyspace()
    }
    catch
    {
      case e: Exception =>
        logger.error(e.getMessage, e)
        "FAIL"
    }
  }

  private [cassandra] def flushStagingKeySpace(keyspaceName: String, flushThreshold: Int): Unit =
  {
    BatchToCassandraFlushStaging.q += Tuple2(keyspaceName, flushThreshold)
  }

  private def stopB2c()
  {
    while(currentCycleRunning)
      {
        logger.info("Waiting for cycle to complete....")

        Thread.sleep(3000)

      }
    logger.info("Shut Down Signal Sent")

    continueRun=false


    threadStarted=false


  }

  private def errorCode(errorMessage: String): String =
  {
    errorMessage match
    {
      case "Could not abstract subset for checksum" => "nodata-"
      case "Too many duplicate records " => "duplicate-"
      case x if (x.length < 25) => x.toLowerCase().replaceAll("[^A-Za-z0-9]", "")
      case _ => errorMessage.substring(0, 24).replace("[^A-Za-z0-9]", "")
    }
  }


  @throws(classOf[IKodaMLException])
  private def saveBatchSupplement(path: org.apache.hadoop.fs.Path): Unit =
  {
    try
    {
      val keySpaceNameo: Option[String] = keySpaceFromPath(path.toString)
      val keyspaceUuido: Option[String] = keySpaceUuidFromPath(path.toString)

      if (keySpaceNameo.isDefined && keyspaceUuido.isDefined && continueRun)
      {
        logger.info("\n------------\nsaveBatchSupplement "+path+"\n------------\n")
        if(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keySpaceNameo.get).supplement) {

          val pconfig: PipelineConfiguration = new PipelineConfiguration
          pconfig.config(PipelineConfiguration.keyspaceName, keySpaceNameo.get)
          pconfig.config(PipelineConfiguration.keyspaceUUID, keyspaceUuido.get)
          val ssmm = new SparseSupplementModelMaker(pconfig)
          ssmm.createTablesIfNotExist(path.toString) match {
            case true =>
              sparseSupplementSet += keySpaceNameo.get
              val ssdi: SparseSupplementDataInput = new SparseSupplementDataInput(pconfig)
              ssdi.insertSupplementaryData(path.toString)
              val pathout = CassandraKeyspaceConfigurationFactory.keyspaceConfig(keySpaceNameo.get).supplementpersisteddir
              movePersistedData(path, new org.apache.hadoop.fs.Path(pathout))
              logger.info("\n\n--------------------\nSaved supplement " + keySpaceNameo.get + "\n--------------------\n")
            case false =>
              logger.warn("Failed to find or create sparseSupplement table in " + keySpaceNameo)
          }
        }
        else
          {
            logger.info("saveBatchSupplement: Configured to ignore supplement")
          }
      }
      else
      {
        logger.warn("Missing input data keySpaceNameo: " + keySpaceNameo + " keyspaceUuido: " + keyspaceUuido)
      }
    }
    catch
    {
      case e: Exception => logger.error(s"\n\nHandled Exception: ${e.getMessage}", e)
        keySpaceFromPath(path.toString).isDefined match {
          case true=>
            val pathout=CassandraKeyspaceConfigurationFactory.keyspaceConfig(keySpaceFromPath(path.toString).get).supplementerrordir
            movePersistedData (path, new org.apache.hadoop.fs.Path(pathout),false, errorCode (e.getMessage) )
          case _ =>
        }
    }
  }


  private def loadLibsvm(path: org.apache.hadoop.fs.Path): Option[RDDLabeledPoint] =
  {

      logger.info("Loading "+path)

      val dirfile = breakPathToDirAndFile(path.toString)
      val tryToOpen:Try[RDDLabeledPoint]=RDDLabeledPoint.loadLibSvm( dirfile._2, dirfile._1)
      tryToOpen.isSuccess match
        {
        case true => Some(tryToOpen.get)
        case _ => tryToOpen.failed.get.getMessage match
          {
          case x if(x.contains("empty collection") || stackTraceString(tryToOpen.failed.get).toLowerCase().contains("numberformat") || stackTraceString(tryToOpen.failed.get).toLowerCase().toLowerCase().contains("Stringops"))=>
            keySpaceFromPath(path.toString).isDefined match {
              case true =>
                val pathout=CassandraKeyspaceConfigurationFactory.keyspaceConfig(keySpaceFromPath(path.toString).get).errordir
                movePersistedData (path, new org.apache.hadoop.fs.Path(pathout),false, x)
                None
              case false => None
        }
      }

    }

  }


  @throws(classOf[IKodaMLException])
  private def saveBatch(path: org.apache.hadoop.fs.Path, keySpaceName: String, keySpaceUuid: String): Unit =
  {
    try
    {
      logger.info("\n------------\nsaveBatch "+path+"\n------------\n")
      val sparse0o = loadLibsvm(path)
      if (sparse0o.isDefined && continueRun)
      {
        CassandraKeyspaceConfigurationFactory.keyspaceConfig(keySpaceName).truncateoverwrite match
          {
            case true =>
              logger.info("\n---------------\nOverrite keyspace set. Overwriting before saving new data\n---------------\n" )
              truncateKeyspace(keySpaceName)
            case _ =>
          }
        val sparse0 = sparse0o.get

        logger.info("Keyspace: " + keySpaceName)
        logger.info(sparse0.info)

        val pconfig: PipelineConfiguration = new PipelineConfiguration
        pconfig.config(PipelineConfiguration.keyspaceName, keySpaceName)
        pconfig.config(PipelineConfiguration.keyspaceUUID, keySpaceUuid)
        val smm: SparseModelMaker = new SparseModelMaker(pconfig)
        smm.createTablesIfNotExist(sparse0)
        logger.info("\n------------\nInserting Data \n------------\n")
        val sdi: SparseDataInput = new SparseDataInput(pconfig)
        sdi.insertAllData(sparse0)
        val outpath=new org.apache.hadoop.fs.Path(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keySpaceName).persistedDir)
        movePersistedData(path,outpath)
        logger.info("\n\n--------------------\nSaved batch " + keySpaceName + "\n--------------------\n")
      }
    }
    catch
    {
      case e: Exception => logger.error(s"\n\nHandled Exception: ${e.getMessage}", e)
        val outpath:org.apache.hadoop.fs.Path=new org.apache.hadoop.fs.Path(CassandraKeyspaceConfigurationFactory.keyspaceConfig(keySpaceName).errordir)
        movePersistedData(path, outpath, false, errorCode(e.getMessage))
    }
  }



  private def movePersistedData(path: org.apache.hadoop.fs.Path, pathout: org.apache.hadoop.fs.Path, success: Boolean = true, error: String = ""): Unit =
  {
    try
    {
      val tuple = breakPathToDirAndFile(path.toString)
      val dirList = SparkDirectoryFinder.directoryStructure(tuple._1)
      logger.debug("movePersistedData: " + path)
      logger.debug("moving if contains: " + tuple._2)

      dirList.filter(d => d.toString.contains(tuple._2)).foreach
      {
        p: org.apache.hadoop.fs.Path =>
          logger.debug("moving: " + p)
          if (success)
          {

            SparkDirectoryFinder.renamePartition(p, new org.apache.hadoop.fs.Path(pathout.toString+File.separator+p.getName))
            logger.debug("moving to: " + pathout.toString+File.separator+p.getName)
          }
          else
          {
            logger.debug("moving to: "+persistFailedPath(p, pathout,error))
            SparkDirectoryFinder.renamePartition(p, persistFailedPath(p,pathout, error))
          }
      }
    }
    catch
    {
      case e: Exception => logger.debug(e.getMessage, e)
    }
  }

  private def persistFailedPath( path: org.apache.hadoop.fs.Path, pathout: org.apache.hadoop.fs.Path, errorMessage: String) : org.apache.hadoop.fs.Path =
  {
     new org.apache.hadoop.fs.Path(pathout.toString + File.separator + errorCode(errorMessage) +File.separator+ path.getName)
  }

  private def keySpaceFromPath(path: String): Option[String] =
  {
    try
    {
      Some(path.substring((path.indexOf("|kss|") + 5), path.indexOf("|kse|")))
    }
    catch
    {
      case e: Exception => logger.error("Could not extract keyspace from: " + path+" with |kss| and |kse|")
        None
    }
  }

  private def keySpaceUuidFromPath(path: String): Option[String] =
  {
    try
    {
      Some(path.substring((path.indexOf("|uuids|") + 7), path.indexOf("|uuide|")))
    }
    catch
    {
      case e: Exception => logger.error("Could not extract keyspace uuid from: " + path + ": " + e.getMessage)
        None
    }

  }
}