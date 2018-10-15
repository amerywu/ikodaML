package ikoda.ml.cassandra

import java.util.UUID

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.cassandra.SparseModelMaker.getSparkSession
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.RDDLabeledPoint
import ikoda.utilobjects.SparkConfProviderWithStreaming
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Try

object SparseModelMaker extends Logging with SparkConfProviderWithStreaming
{


  @throws(classOf[IKodaMLException])
  def keyspaceExists(keyspaceIn:String):Boolean =
  {
    import com.datastax.spark.connector._

    val sqlC = getSparkSession().sqlContext



    try
    {
      val df = getSparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "targetMap", "keyspace" -> keyspaceIn))
        .load()



      true
    }
    catch
      {

        case e:Exception=>
          false


      }

  }
}

class SparseModelMaker(pconfig:PipelineConfiguration) extends Logging  with QueryExecutor
{
  val keyspace:String=pconfig.get(PipelineConfiguration.keyspaceName)
  lazy val batchuuid:String=UUIDs.timeBased().toString
  lazy val replicationFactor:Int=ConfigFactory.load("streaming").getInt("streaming.cassandraconfig.rf")

  @throws(classOf[IKodaMLException])
  def createTablesIfNotExist(sparse0: RDDLabeledPoint): String =
  {
    executeQuery("CREATE KEYSPACE IF NOT EXISTS "+keyspace + s" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")

    createDataTable(sparse0)
    createColumnTable(sparse0)
    createTargetTable(sparse0)
    createSentenceToWordMap()

    createKeyspaceUUIDTable()

  }

  def createNewKeyspace(): String =
  {
    executeQuery("CREATE KEYSPACE IF NOT EXISTS "+keyspace + s" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")
    createKeyspaceUUIDTable()
  }


  @throws(classOf[IKodaMLException])
  private def createDataTable(sparse0: RDDLabeledPoint)
  {
    import com.datastax.spark.connector._

    val sqlC = getSparkSession.sqlContext

    import sqlC.implicits._
    //val dfOut:DataFrame = osparse.get.sparseData().toDF
    import com.datastax.driver.core.utils.UUIDs

    try
    {
      val df = getSparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "sparseData", "keyspace" -> keyspace))
        .load

    }
    catch
    {

      case e:Exception=>
        logger.warn(e.getMessage)

        logger.info(s"\n----------\nCreating sparseData in $keyspace\n----------\n")
        val label = sparse0.sparseData().first()._1.label
        val idcs: Seq[Int] = sparse0.sparseData().first._1.features.toSparse.indices.toSeq
        val values: Seq[Double] = sparse0.sparseData().first._1.features.toSparse.values.toSeq
        val hashcode:Long=sparse0.sparseData().first().hashCode()
        val timestamp:Long=System.currentTimeMillis()




        //("UUID",UUIDs.timeBased()),


        val dfOut: DataFrame = Seq((UUIDs.timeBased().toString, label, idcs, values, hashcode,timestamp,batchuuid)).toDF(
          "uuid", "label", "indices", "values","hashcode","timestamp","batchuuid"
        )
        dfOut.createCassandraTable(
          keyspace,
          "sparseData",
          partitionKeyColumns = Some(Seq("label")),
          clusteringKeyColumns = Some(Seq("uuid"))
        )


        executeQuery("CREATE INDEX ON "+keyspace+".\"sparseData\"(hashcode)")
        executeQuery("CREATE INDEX ON "+keyspace+".\"sparseData\"(batchuuid)")


        logger.info(keyspace+".sparseData        Table created.")

    }

  }

  @throws(classOf[IKodaMLException])
  private def createDataTableBoW(sparse0: RDDLabeledPoint)
  {
    import com.datastax.spark.connector._

    val sqlC = getSparkSession.sqlContext
    //val dfOut:DataFrame = osparse.get.sparseData().toDF

    try
    {



      val df = getSparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "bagOfWords", "keyspace" -> keyspace))
        .load()



    }
    catch
    {

      case e:Exception=>
        logger.warn(e.getMessage)
        val label = sparse0.sparseData().first()._1.label




        //("UUID",UUIDs.timeBased()),


        val termColumns:Seq[StructField]=sparse0.columnHeads().map{

          cht => StructField(cht.stringLabel.toLowerCase(), DoubleType, true)
        }

        val allFields:Seq[StructField]= Seq(StructField("targetlabel",DoubleType,false),StructField("uuid",StringType,false),StructField("timestamp",LongType, false),StructField("batchuuid",StringType, false)) ++ termColumns


        val schema:StructType=new StructType(allFields.toArray)
        val dataRDD = getSparkSession.sparkContext.emptyRDD[Row]
        val dfOut = getSparkSession.createDataFrame(dataRDD, schema)

        dfOut.createCassandraTable(
          keyspace,
          "bagOfWords",
          partitionKeyColumns = Some(Seq("targetlabel")),
          clusteringKeyColumns = Some(Seq("uuid","batchuuid"))
        )

        executeQuery("CREATE INDEX ON "+keyspace+".\"bagOfWords\"(batchuuid)")
        logger.info("Table created")
    }

  }








  @throws(classOf[IKodaMLException])
  private def createColumnTable(sparse0: RDDLabeledPoint)
  {
    import com.datastax.spark.connector._

    val sqlC = getSparkSession.sqlContext

    import sqlC.implicits._
    //val dfOut:DataFrame = osparse.get.sparseData().toDF

    try
    {
      val df = getSparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "columnMap", "keyspace" -> keyspace))
        .load()




    }
    catch
    {

      case e:Exception=>
        logger.warn(e.getMessage)

        //("UUID",UUIDs.timeBased()),
        val inputDataSeq:Seq[(String,Int, String)]=sparse0.columnHeads().map
        {
          ch => (cleanColumnName(ch.stringLabel.toLowerCase()),ch.numericLabel, batchuuid)
        }

        val dfOut: DataFrame = inputDataSeq.toDF(
          "columnName", "columnIndex", "batchuuid"
        )

        dfOut.createCassandraTable(
          keyspace,
          "columnMap",
          partitionKeyColumns = Some(Seq("columnName")),
          clusteringKeyColumns = Some(Seq("batchuuid"))
        )

        logger.info(keyspace+".columnMap Table created")


        dfOut.write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "columnMap", "keyspace" -> keyspace))
          .save()

        logger.info("Column Names Inserted")

        executeQuery("CREATE INDEX ON "+keyspace+".\"columnMap\"(\"columnIndex\")")
        executeQuery("CREATE INDEX ON "+keyspace+".\"columnMap\"(batchuuid)")
    }

  }

  @throws(classOf[IKodaMLException])
  private def createTargetTable(sparse0: RDDLabeledPoint)
  {
    import com.datastax.spark.connector._

    val sqlC = getSparkSession.sqlContext

    import sqlC.implicits._
    //val dfOut:DataFrame = osparse.get.sparseData().toDF

    try
    {
      val df = getSparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "targetMap", "keyspace" -> keyspace))
        .load()



    }
    catch
    {

      case e:Exception=>
        logger.warn(e.getMessage)




        //("UUID",UUIDs.timeBased()),
        val inputDataSeq:Seq[(String,Double,String)]=sparse0.getTargets().map
        {
          e => (e._1.toLowerCase(),e._2,batchuuid)
        }.toSeq

        val dfOut: DataFrame = inputDataSeq.toDF(
          "targetName", "targetIndex", "batchuuid"
        )
        dfOut.createCassandraTable(
          keyspace,
          "targetMap",
          partitionKeyColumns = Some(Seq("targetName")),
          clusteringKeyColumns = Some(Seq("targetIndex"))
        )
        logger.info(keyspace+".targetMap Table created")

        dfOut.write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "targetMap", "keyspace" -> keyspace))
          .save()

        executeQuery("CREATE INDEX ON "+keyspace+".\"targetMap\"(batchuuid)")
        logger.info(keyspace+" Target Names Inserted")


    }

  }


  @throws(classOf[IKodaMLException])
  private def createKeyspaceUUIDTable():String =
  {
    import com.datastax.spark.connector._

    val sqlC = getSparkSession.sqlContext

    import com.datastax.driver.core.utils.UUIDs
    import sqlC.implicits._

    try
    {
      val df = getSparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "keyspaceuuid", "keyspace" -> keyspace))
        .load()



      "EXTANT"
    }
    catch
    {

      case e:Exception=>
        logger.warn(e.getMessage)

        //("UUID",UUIDs.timeBased()),
        val uuid:String=CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).uuid.isEmpty match
        {
          case false => CassandraKeyspaceConfigurationFactory.keyspaceConfig(keyspace).uuid
          case true => UUID.randomUUID().toString

        }
        val inputDataSeq:Seq[(String)]=Seq(uuid)

        val dfOut: DataFrame = inputDataSeq.toDF(
          "uuid"
        )
        dfOut.createCassandraTable(
          keyspace,
          "keyspaceuuid",
          partitionKeyColumns = Some(Seq("uuid"))
        )
        logger.info(keyspace+".keyspaceuuid created "+uuid)

        dfOut.write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "keyspaceuuid", "keyspace" -> keyspace))
          .save()
        logger.info("UUID Inserted: "+uuid)
        uuid
    }

  }



  @throws(classOf[IKodaMLException])
  private def createSentenceToWordMap()
  {
    import com.datastax.spark.connector._

    val sqlC = getSparkSession.sqlContext

    import sqlC.implicits._
    //val dfOut:DataFrame = osparse.get.sparseData().toDF
    import com.datastax.driver.core.utils.UUIDs
    try
    {
      val df = getSparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "sentenceToWordMap", "keyspace" -> keyspace))
        .load()


    }
    catch
    {
      case e:Exception=>
        logger.warn(e.getMessage)
        val inputDataSeq:Seq[(String,String,Int,Long, String)]=Seq((UUIDs.timeBased().toString, UUIDs.timeBased().toString, -1,System.currentTimeMillis(),batchuuid))

        val dfOut: DataFrame = inputDataSeq.toDF(
          "uuid","sentenceUuid", "columnIndex","timestamp","batchuuid"
        )
        dfOut.createCassandraTable(
          keyspace,
          "sentenceToWordMap",
          partitionKeyColumns = Some(Seq("columnIndex")),
          clusteringKeyColumns = Some(Seq("sentenceUuid","uuid"))
        )

        executeQuery("CREATE INDEX ON "+keyspace+".\"sentenceToWordMap\"(\"sentenceUuid\")")
        executeQuery("CREATE INDEX ON "+keyspace+".\"sentenceToWordMap\"(batchuuid)")
        logger.info(keyspace+".sentenceToWordMap Table created")
    }

  }

}
