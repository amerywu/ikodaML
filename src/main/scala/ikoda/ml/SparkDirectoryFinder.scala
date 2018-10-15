package ikoda.ml

import java.io.File

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.cassandra.CassandraKeyspaceConfigurationFactory
import ikoda.utilobjects.SparkConfProviderWithStreaming

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try



object SparkDirectoryFinder extends Logging with SparkConfProviderWithStreaming with DirectoryUtils
{

  lazy val modelRootDiro: Try[String] = Try(ConfigFactory.load("scalaML").getString("scalaML.modelsRoot.value"))

  lazy val mlpcPredictByUrlModelNameo: Try[String] = Try(
    ConfigFactory.load("scalaML").getString("scalaML.mlpcPredictByUrlModelName.value")
  )

  lazy val schemaFileNameo: Try[String] = Try(
    ConfigFactory.load("scalaML").getString("scalaML.mlpcDataSchema.value")
  )

  lazy val fsRoot: Try[String] = Try(
  ConfigFactory.load("streaming").getString("streaming.root.fsRoot")
)
  lazy val topDir: Try[String] = Try(
    ConfigFactory.load("streaming").getString("streaming.root.topDir")
  )

  initializeFileSystem
  info

  @throws(classOf[IKodaMLException])
  def initializeFileSystem(): Unit =
  {
    try
    {
      fsRoot.isSuccess match {

        case true=>
          val conf = new org.apache.hadoop.conf.Configuration ()
          val fs = org.apache.hadoop.fs.FileSystem.get (new java.net.URI (fsRoot.get), conf)
          fs.exists(new org.apache.hadoop.fs.Path(topDir.getOrElse("ikoda"))) match
            {
            case true => logger.info(topDir.getOrElse("ikoda")+ " exists")
            case false =>
              fs.mkdirs(new org.apache.hadoop.fs.Path(topDir.getOrElse("ikoda")))

          }

          fs.exists(new org.apache.hadoop.fs.Path(modelRootDiro.getOrElse("/ikoda/predmodels"))) match
          {
            case true =>logger.info(modelRootDiro.getOrElse("/ikoda/predmodels")+ " exists")
            case false => fs.mkdirs(new org.apache.hadoop.fs.Path(modelRootDiro.getOrElse("/ikoda/predmodels")))

          }
          modelRootDiro

        case false => throw (fsRoot.failed.get)
      }
    }
    catch
      {

        case e:Exception =>
          logger.info(e.getMessage,e)
          throw new IKodaMLException(e.getMessage,e)
      }
  }

  @throws(classOf[IKodaMLException])
  def info: Unit =
  {
    val sb:StringBuilder=new StringBuilder
    try
    {
        sb.append("\n\nFile System Info:\n\n")
        sb.append("\nmodelRootDiro : "+modelRootDiro)
        sb.append("\nfsRoot : "+fsRoot)
        sb.append("\nmlpcPredictByUrlModelNameo : "+mlpcPredictByUrlModelNameo)
        sb.append("\nschemaFileNameo : "+schemaFileNameo)


        sb.append("\nStats:")
        org.apache.hadoop.fs.FileSystem.getAllStatistics.asScala.foreach
        {
          stats => sb.append("\n"+stats.getScheme)
                   sb.append("\n"+stats)
        }

        sb.append("\n")

     fsRoot.isSuccess match
     {
       case true =>
         val conf = new org.apache.hadoop.conf.Configuration ()
         val path=new org.apache.hadoop.fs.Path(topDir.getOrElse("ikoda"))

         //val fs = org.apache.hadoop.fs.FileSystem.get (new java.net.URI (fsRoot.get), conf)
         val fs = path.getFileSystem(getSparkSession().sparkContext.hadoopConfiguration)
         sb.append("\nPaths:")
         val buffer:ArrayBuffer[org.apache.hadoop.fs.Path]=new ArrayBuffer[org.apache.hadoop.fs.Path]
         directoryStructure(new org.apache.hadoop.fs.Path("/"),buffer)
         sb.append(buffer.mkString("\n"))
         sb.append("\nFiles:")
         val files = fs.listFiles (new org.apache.hadoop.fs.Path(topDir.getOrElse("ikoda")), true)
         while (files.hasNext)
         {
             val f: org.apache.hadoop.fs.LocatedFileStatus = files.next ()
             sb.append ("\n" + f.getPath)
             sb.append (" " + fs.isDirectory (f.getPath) )
         }

   case false => throw (fsRoot.failed.get)
   }
        logger.info (sb)

    }
    catch
      {

        case e:Exception =>
          logger.info(sb)
          throw new IKodaMLException(e.getMessage,e)
      }
  }

  @throws(classOf[IKodaMLException])
  def cassandraBatchDirectoriesSparse(): Seq[String] =
  {
      CassandraKeyspaceConfigurationFactory.streamingDirectories().toSeq
  }



  @throws(classOf[IKodaMLException])
  def cassandraBatchSupplementDirectoriesSparse(): Seq[String] =
  {
     CassandraKeyspaceConfigurationFactory.supplementStreamingDirectories().toSeq
  }

  private def generateSuffix(fileNameIn: String): String =
  {
    if (fileNameIn.contains("-targetMap"))
    {
      "-targetMap"
    }
    else if (fileNameIn.contains("-columnMap"))
    {
      "-columnMap"
    }
    else
    {
      ""
    }
  }

}
