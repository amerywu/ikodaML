package ikoda.ml.cassandra

import java.io.File

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.DirectoryUtils
import ikoda.utilobjects.SparkConfProviderWithStreaming
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

object DataSetCache extends SparkConfProviderWithStreaming with Logging with DirectoryUtils {


  lazy val fsRoot: Try[String] = Try(
    ConfigFactory.load("streaming").getString("streaming.root.fsRoot"))

  lazy val topDir: Try[String] = Try(
    ConfigFactory.load("streaming").getString("streaming.root.topDir")
  )

  var timestamp: String = System.currentTimeMillis().toString
  var lastClean: Long = System.currentTimeMillis()

  lazy val cacheDir: org.apache.hadoop.fs.Path = init

  private def init: org.apache.hadoop.fs.Path = {
    try {

      def asRoot(s: String): String = {
        s.startsWith(File.separator) match {
          case true => s
          case _ => File.separator + s

        }
      }

      fsRoot.isSuccess match {
        case true =>
          val conf = new org.apache.hadoop.conf.Configuration()
          val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fsRoot.get), conf)
          fs.exists(new org.apache.hadoop.fs.Path(topDir.getOrElse("ikoda"))) match {
            case true => logger.info(topDir.getOrElse(File.separator + "ikoda") + " exists")
            case false =>
              fs.mkdirs(new org.apache.hadoop.fs.Path(asRoot(topDir.getOrElse(File.separator + "ikoda"))))
          }
          fs.exists(new org.apache.hadoop.fs.Path(topDir.getOrElse(File.separator + "ikoda") + File.separator + "temp")) match {
            case true =>
            case false =>
              fs.mkdirs(new org.apache.hadoop.fs.Path(topDir.getOrElse(File.separator + "ikoda") + File.separator + "temp"))

          }
          val pathOut = new org.apache.hadoop.fs.Path(asRoot(topDir.getOrElse(File.separator + "ikoda") + File.separator + "temp"))
          logger.info("Cache location: " + pathOut)
          pathOut

        case false =>
          val conf = new org.apache.hadoop.conf.Configuration()
          val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(File.separator), conf)
          fs.mkdirs(new org.apache.hadoop.fs.Path(File.separator + "temp"))
          new org.apache.hadoop.fs.Path(File.separator + "temp")
      }
    }
    catch {
      case e: Exception => logger.error(e.getMessage, e)
        throw new IKodaMLException(e.getMessage, e)
    }
  }

  def validData(df: DataFrame): Boolean = {

    val errorMessage = "Cached DataFrame must include 'dsctimestamp' of type Long"

    df.schema.fieldNames.contains("dsctimestamp") match {
      case true =>
        val sfts = df.schema.filter(sf => sf.name.equals("dsctimestamp"))(0)
        val pass = sfts.dataType == LongType
        pass match {
          case true => pass
          case false => throw new IKodaMLException(errorMessage)
        }
      case false => throw new IKodaMLException(errorMessage)
    }
  }


  def addToCache(df: DataFrame, name: String, ttlMinutes: Int = 1440): Unit = {
    try {
      synchronized {
        validData(df)


        appendDataFrame(df, cacheDir + File.separator + name)
        cleanCache(name, ttlMinutes)

      }

    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  def overWriteCache(df: DataFrame, name: String): Unit = {
    try {
      logger.debug("overWriteCache " + name)
      validData(df)
      saveDataFrame(df, cacheDir + File.separator + name)
    }
    catch {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }

  }


  def get(name: String): Option[DataFrame] = {
    synchronized {
      logger.debug("loading cache from " + cacheDir + File.separator + name)
      val dft = load(cacheDir + File.separator + name)

      dft.isSuccess match {
        case true =>


          Some(dft.get)
        case false => None
      }
    }
  }



  private def removeOldCachePartitions(name: String, dfOut: DataFrame, ttlMillis: Long): Unit = {
    try {


      def pathSeq: Seq[org.apache.hadoop.fs.Path] = directoryContents(new org.apache.hadoop.fs.Path(cacheDir + File.separator + name))

      logger.debug("removeOldCachePartitions " + pathSeq.mkString(","))
      pathSeq.isEmpty match {
        case true =>
        case false =>
          val oldpaths = pathSeq.filter(p => !p.toString.contains(timestamp))
          oldpaths.isEmpty match {
            case true =>
            case false =>
              oldpaths.foreach {
                p =>
                  logger.debug("modificationTime" + modificationTime(p))
                  if (modificationTime(p) < System.currentTimeMillis() - ttlMillis) {
                    deletePartition(p.toString)
                  }

              }
          }

      }
      DataSetCache.overWriteCache(dfOut, name)
    }
    catch {
      case e: Exception => logger.warn("Handled but bad: " + e.getMessage, e)
    }
  }

  def cleanCache(name: String, ttlMinutes: Int): Unit = {

    ttlMinutes > 0 match {
      case true =>
        try {
          if (System.currentTimeMillis() - lastClean > 360000) {
            val dfo = get(name)
            if (dfo.isDefined) {
              val df = dfo.get
              val countApprox: Double = df.rdd.countApprox(1000, 0.95).getFinalValue().mean
              lastClean = System.currentTimeMillis()
              val ttlMillis: Long = (ttlMinutes * 60) * 1000.toLong
              logger.debug("cleanCache unclean " + countApprox)
              if (countApprox > 30000) {
                val dfOut: DataFrame = ttlMillis > 0 match {

                  case true => df.filter(r => r.getAs[Long]("dsctimestamp") > (System.currentTimeMillis() - ttlMillis))
                  case false => df
                }
                val countApproxf: Double = dfOut.rdd.countApprox(1000, 0.95).getFinalValue().mean
                logger.debug("cleanCache After filter " + countApproxf)
                removeOldCachePartitions(name, dfOut, ttlMillis)
              }
            }
          }
        }
        catch {
          case e: Exception => logger.warn("Handled but bad: " + e.getMessage, e)
        }
      case false =>
    }
  }

  private def load(path: String): Try[DataFrame] = {
    synchronized {
      logger.debug("Loading from cache " + path)

      def pathSeq: Seq[org.apache.hadoop.fs.Path] = directoryContents(new org.apache.hadoop.fs.Path(path))

      logger.debug("Loading cache " + pathSeq.mkString(","))
      pathSeq.isEmpty match {
        case true =>
          logger.warn("cache is empty")
          Try(throw new IKodaMLException("Cache is empty"))
        case false =>

          val po = currentPartition(pathSeq)
          po.isDefined match {
            case true =>
              logger.debug("loading from " + po)
              Try(spark.read.option("header", "true").option("inferSchema", "true").csv(po.get.toString))
            case false => Try(throw new IKodaMLException("Cache not found"))
          }
      }
    }
  }


  private def saveDataFrame(df: DataFrame, path: String): Unit = {
    synchronized {
      try {
        timestamp = System.currentTimeMillis().toString
        logger.info("New underlying hdfs file for cache: " + path + File.separator + timestamp)
        df.write.mode(SaveMode.Append).option("header", "true").csv(path + File.separator + timestamp)
      }
      catch {
        case e: Exception => logger.warn("\nsaveDataFrame.  \nHandled but bad: " + e.getMessage, e)
      }
    }
  }

  private def appendDataFrame(df: DataFrame, path: String): Unit = {
    synchronized {
      val countApprox: Double = df.rdd.countApprox(1000, 0.95).getFinalValue().mean
      logger.debug("adding rows to cache " + countApprox)

      def pathSeq: Seq[org.apache.hadoop.fs.Path] = directoryContents(new org.apache.hadoop.fs.Path(path))

      logger.debug("Appending to cache " + pathSeq.mkString(","))
      pathSeq.isEmpty match {
        case true => df.write.mode(SaveMode.Append).option("header", "true").csv(path + File.separator + timestamp)
        case false =>
          val po = currentPartition(pathSeq)
          po.isDefined match {
            case true =>
              logger.debug("Using current cache: " + po.get)
              df.write.mode(SaveMode.Append).option("header", "true").csv(po.get.toString)
            case false => timestamp = System.currentTimeMillis().toString
              df.write.mode(SaveMode.Append).option("header", "true").csv(path + File.separator + timestamp)
          }
      }
    }
  }

  private def currentPartition(pathSeq: Seq[org.apache.hadoop.fs.Path]): Option[org.apache.hadoop.fs.Path] = {
    pathSeq.find(p => p.toString.contains(timestamp))
  }


}
