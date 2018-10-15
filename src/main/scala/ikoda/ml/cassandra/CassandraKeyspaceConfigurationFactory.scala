package ikoda.ml.cassandra

import com.typesafe.config.{Config, ConfigFactory, ConfigList, ConfigObject}
import grizzled.slf4j.Logging
import ikoda.IKodaMLException

import scala.collection.JavaConverters._
import scala.util.Try

object CassandraKeyspaceConfigurationFactory extends Logging {

  private val keyspaceConfig:Config = ConfigFactory.load("keyspaces").getConfig("keyspaces")
  private lazy val ksl:Set[String]=keyspaceConfig.root().keySet().asScala.toSet
  private lazy val ckcMap: Map[String,CassandraKeyspaceConfiguration]=load
  lazy val fsRoot: Try[String] = Try(ConfigFactory.load("scalaML").getString("scalaML.root.fsRoot"))
  initializeFileSystem()


  def info(): Unit =
  {

    val sb:StringBuilder = new StringBuilder
    ksl.foreach{
      ks=> val ksinfo=ConfigFactory.load("keyspaces").getConfig(s"keyspaces.$ks")
        sb.append(s"\n$ks.flush: ${Try(ksinfo.getBoolean("flush")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.trim: ${Try(ksinfo.getBoolean("trim")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.supplement: ${Try(ksinfo.getBoolean("supplement")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.hdfs: ${Try(ksinfo.getBoolean("hdfs")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.truncateOverwrite: ${Try(ksinfo.getBoolean("truncateOverwrite")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.flushThreshold: ${Try(ksinfo.getInt("flushThreshold")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.rf: ${Try(ksinfo.getInt("rf")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.dir: ${Try(ksinfo.getString("dir")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.persistedDir: ${Try(ksinfo.getString("persistedDir")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.errordir: ${Try(ksinfo.getString("errorDir")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.supplementDir: ${Try(ksinfo.getString("supplementDir")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.supplementPersistedDir: ${Try(ksinfo.getString("supplementPersistedDir")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.supplementErrorDir: ${Try(ksinfo.getString("supplementErrorDir")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.uuid: ${Try(ksinfo.getString("uuid")).getOrElse("undefined ergo default")}")
        sb.append(s"\n$ks.permittedProportionOfDuplicates: ${Try(ksinfo.getString("permittedProportionOfDuplicates")).getOrElse("undefined ergo default")}\n-----\n")
    }
    logger.info(sb)
  }


  def initializeFileSystem(): Unit =
  {
    try
    {
      load()
      fsRoot.isSuccess match {

        case true =>
          ckcMap.foreach {
            ckc=>
                val conf = new org.apache.hadoop.conf.Configuration()
                val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(fsRoot.get), conf)
                fs.exists(new org.apache.hadoop.fs.Path(ckc._2.errordir)) match
                {
                  case true =>
                  case false =>
                    fs.mkdirs(new org.apache.hadoop.fs.Path(ckc._2.errordir))

                }
              fs.exists(new org.apache.hadoop.fs.Path(ckc._2.dir)) match
            {
              case true =>
              case false =>
                fs.mkdirs(new org.apache.hadoop.fs.Path(ckc._2.dir))

            }
              fs.exists(new org.apache.hadoop.fs.Path(ckc._2.archivedir)) match
              {
                case true =>
                case false =>
                  fs.mkdirs(new org.apache.hadoop.fs.Path(ckc._2.archivedir))

              }
              fs.exists(new org.apache.hadoop.fs.Path(ckc._2.supplementdir)) match
              {
                case true =>
                case false =>
                  fs.mkdirs(new org.apache.hadoop.fs.Path(ckc._2.supplementdir))

              }
              fs.exists(new org.apache.hadoop.fs.Path(ckc._2.supplementpersisteddir)) match
              {
                case true =>
                case false =>
                  fs.mkdirs(new org.apache.hadoop.fs.Path(ckc._2.supplementpersisteddir))

              }
              fs.exists(new org.apache.hadoop.fs.Path(ckc._2.persistedDir)) match
              {
                case true =>
                case false =>
                  fs.mkdirs(new org.apache.hadoop.fs.Path(ckc._2.persistedDir))

              }
              fs.exists(new org.apache.hadoop.fs.Path(ckc._2.supplementerrordir)) match
              {
                case true =>
                case false =>
                  fs.mkdirs(new org.apache.hadoop.fs.Path(ckc._2.supplementerrordir))

              }
      }
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


  def load(): Map[String,CassandraKeyspaceConfiguration] =
  {
    ksl.map
    {
      ks =>
        val ksinfo=ConfigFactory.load("keyspaces").getConfig(s"keyspaces.$ks")
        val ckc=new CassandraKeyspaceConfiguration(
          Try(ksinfo.getBoolean("flush")),
          Try(ksinfo.getBoolean("trim")),
          Try(ksinfo.getBoolean("supplement")),
          Try(ksinfo.getBoolean("hdfs")),
          Try(ksinfo.getBoolean("truncateOverwrite")),
          Try(ksinfo.getBoolean("archive")),
          Try(ksinfo.getInt("flushThreshold")),
          Try(ksinfo.getInt("rf")),
          Try(ksinfo.getString("uidInSparse")),
          Try(ksinfo.getString("dir")),
          Try(ksinfo.getString("persistedDir")),
          Try(ksinfo.getString("errorDir")),
          Try(ksinfo.getString("supplementDir")),
          Try(ksinfo.getString("supplementPersistedDir")),
          Try(ksinfo.getString("supplementErrorDir")),
          Try(ksinfo.getString("archiveDir")),
          Try(ksinfo.getString("supplementSuffix")),
          Try(ksinfo.getString("uuid")),
          Try(ksinfo.getDouble("permittedProportionOfDuplicates"))
      )

        ks -> ckc
    }.toMap
  }

  @throws(classOf[IKodaMLException])
  def keyspaceConfig(ks:String ): CassandraKeyspaceConfiguration =
  {


    ckcMap.get(ks).isDefined match
      {
      case true => ckcMap.get(ks).get
      case false =>
        ckcMap.get("defaultks").isDefined match
          {
          case true =>
            logger.warn(s"$ks not defined. Using default configuration")
            ckcMap.get("defaultks").get
          case false => throw new IKodaMLException(s"$ks is not defined in keyspaces.conf AND defaultks is not defined in keyspaces.conf")
        }
    }
  }

  def streamingDirectories(): Set[String] =
  {
    ckcMap.map(ckc => ckc._2.dir).toSet
  }
  def supplementStreamingDirectories(): Set[String] =
  {
    ckcMap.map(ckc => ckc._2.supplementdir).toSet
  }

  def keySpaceNames():Seq[String]=
  {
    ckcMap.map(ckc => ckc._1).toSeq
  }



}
