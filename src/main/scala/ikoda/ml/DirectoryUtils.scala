package ikoda.ml

import java.io.File

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.cassandra.DataSetCache.fsRoot
import ikoda.ml.SparkDirectoryFinder.getSparkSession
import ikoda.utilobjects.SparkConfProviderWithStreaming

import scala.collection.mutable.ArrayBuffer

trait DirectoryUtils extends SparkConfProviderWithStreaming with Logging{



  @throws(classOf[IKodaMLException])
  def pathsOlderThan(millis:Long,path:String): Seq[org.apache.hadoop.fs.Path] =
  {

    pathsOlderThan(millis, new org.apache.hadoop.fs.Path(path))
  }

  @throws(classOf[IKodaMLException])
  def pathsOlderThan(millis:Long,path:org.apache.hadoop.fs.Path): Seq[org.apache.hadoop.fs.Path] =
  {
    try
    {
      val fs = path.getFileSystem(getSparkSession().sparkContext.hadoopConfiguration)
      val buffer:ArrayBuffer[org.apache.hadoop.fs.Path]=new ArrayBuffer[org.apache.hadoop.fs.Path]

      directoryContents(path).filter( p=> fs.getFileStatus(p).getModificationTime < millis)
        .filter(p=>fs.isDirectory(p))
    }
    catch
      {
        case e:Exception=> throw new IKodaMLException(e.getMessage,e)
      }

  }

  @throws(classOf[IKodaMLException])
  def directoryStructure(inpath:String, includeFiles:Boolean=false): Seq[org.apache.hadoop.fs.Path] =
  {
    try {

      val path=new org.apache.hadoop.fs.Path(inpath)
      val buffer:ArrayBuffer[org.apache.hadoop.fs.Path]= new ArrayBuffer[org.apache.hadoop.fs.Path]
      directoryStructure(path,buffer)
      val fs = path.getFileSystem(getSparkSession().sparkContext.hadoopConfiguration)
      includeFiles match {
        case false => buffer.filter (p => fs.isDirectory (p) )
        case true => buffer
      }


    }
    catch
      {
        case e:Exception=> throw new IKodaMLException(e.getMessage,e)
      }
  }



  def modificationTime(path:org.apache.hadoop.fs.Path):Long=
  {
    val conf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fsRoot.get), conf)
    fs.getFileStatus(path).getModificationTime
  }

  @throws(classOf[IKodaMLException])
  def directoryContents(path:org.apache.hadoop.fs.Path,includeFiles:Boolean=true): Seq[org.apache.hadoop.fs.Path] =
  {
    try {
      val fs = path.getFileSystem(getSparkSession().sparkContext.hadoopConfiguration)

      val paths:Seq[org.apache.hadoop.fs.Path]=fs.exists(path ) match {
        case true =>
          val status = fs.listStatus (path)
          status.map {
            x => x.getPath
          }
        case false =>  Seq()
      }

      includeFiles match
      {
        case true => paths
        case false => paths.filter(p=> fs.isDirectory(p))
      }
    }
    catch
      {
        case e:Exception=> throw new IKodaMLException(e.getMessage,e)
      }
  }


  @throws(classOf[IKodaMLException])
  def directoryStructure(path:org.apache.hadoop.fs.Path, buffer:ArrayBuffer[org.apache.hadoop.fs.Path]): Unit =
  {
    try {
      val fs = path.getFileSystem(getSparkSession().sparkContext.hadoopConfiguration)


      fs.exists(path ) match {
        case true =>
          val status = fs.listStatus (path)
          status.foreach {
            x =>
              buffer += x.getPath
              if (! x.getPath.toString.equals (path.toString) ) {
                directoryStructure (x.getPath, buffer)
              }
          }
        case false =>
      }
    }
    catch
      {
        case e:Exception=> throw new IKodaMLException(e.getMessage,e)
      }
  }


  @throws(classOf[IKodaMLException])
  def renamePartition(pathOld: String, pathNew:String):Unit=
  {
    try
    {
      renamePartition(new org.apache.hadoop.fs.Path(pathOld), new org.apache.hadoop.fs.Path(pathNew))
    }
    catch
      {
        case e:Exception => throw new IKodaMLException(e.getMessage,e)
      }
  }


  def mkPartition(pathNew:org.apache.hadoop.fs.Path):Unit=
  {
    try
    {

      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

      hdfs.mkdirs(new org.apache.hadoop.fs.Path(pathNew.toString.substring(0,pathNew.toString.lastIndexOf(File.separator))) )

    }
    catch
      {
        case ex: Exception =>
        {
          logger.debug(s"There has been an Exception mkPartition  $pathNew. Message is ${ex.getMessage} ")

        }
      }

  }


  def renamePartition(pathOld: org.apache.hadoop.fs.Path, pathNew:org.apache.hadoop.fs.Path): Unit =
  {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)



    logger.info(s"Renaming $pathOld to $pathNew")


    try
    {
      mkPartition(pathNew)

      val b=hdfs.rename(pathOld, pathNew)
      logger.debug(s"Rename status: $b")
      b match
      {
        case true =>logger.debug("move Succeeded")
        case false=>logger.warn("Failed to Move to "+pathNew)
      }


    }
    catch
      {
        case ex: Exception =>
        {
          logger.debug(s"There has been an Exception renamePartition $pathOld to $pathNew. Message is ${ex.getMessage} and ${ex}")

        }
      }
  }

  def deletePartition(path: String): Unit =
  {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    logger.info(s"Deleting $path")
    try
    {
      if(hdfs.exists(new org.apache.hadoop.fs.Path(path))) {
        hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
        println("....deleted....")
      }

    }
    catch
      {
        case ex: Exception =>
        {
          logger.debug(s"There has been an Exception during deletion. Message is ${ex.getMessage} and ${ex}")

        }
      }
  }





}
