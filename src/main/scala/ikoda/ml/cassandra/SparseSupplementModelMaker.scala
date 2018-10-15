package ikoda.ml.cassandra

import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.ColumnHeadTuple
import ikoda.utilobjects.DataFrameUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Success}

class SparseSupplementModelMaker(pconfig: PipelineConfiguration) extends Logging  with
  QueryExecutor
{
  val keyspace: String = pconfig.get(PipelineConfiguration.keyspaceName)
  val sqlC = getSparkSession().sqlContext
  lazy val replicationFactor:Int=ConfigFactory.load("streaming").getInt("streaming.cassandraconfig.rf")
  @throws(classOf[IKodaMLException])
  def createTablesIfNotExist(pathToCsv: String): Boolean =
  {
    executeQuery(
      "CREATE KEYSPACE IF NOT EXISTS " + keyspace + s" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }"
    )
    createDataTable(pathToCsv, "sparseSupplement")
  }

  @throws(classOf[IKodaMLException])
  def createTablesIfNotExist(pathToCsv: String, tableName: String): Boolean =
  {
    executeQuery(
      "CREATE KEYSPACE IF NOT EXISTS " + keyspace + s" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' :  ${replicationFactor} }"
    )
    createDataTable(pathToCsv, tableName)
  }


  @throws(classOf[IKodaMLException])
  def createTablesIfNotExist(df: DataFrame): Boolean =
  {
    executeQuery(
      "CREATE KEYSPACE IF NOT EXISTS " + keyspace +s" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' :  ${replicationFactor} }"
    )
    createDataTable(df, "sparseSupplement")
  }



  @throws(classOf[IKodaMLException])
  private def createDataTable(csvPath: String, tableName: String):Boolean=
  {
    try
    {
      val df = getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName, "keyspace" -> keyspace))
        .load()


      logger.info(s"$keyspace.$tableName exists")
      true
    }
    catch
    {

      case e: Exception =>
        logger.warn(e.getMessage)
        val df = getSparkSession().read.format("csv").option("header", "true").load(csvPath)
        tableCreationProcess( tableName, df)


    }
  }


  def supplementExists(): Boolean =
  {
    try
    {
      executeQuery(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name='" + pconfig.get(
          PipelineConfiguration.keyspaceName) + "' and table_name = 'sparseSupplement';") match
      {
        case Success(rs) =>
          logger.debug("Sparse supplement table exists in " + pconfig.get(
            PipelineConfiguration.keyspaceName) + " " + rs.one.getColumnDefinitions)
          true
        case Failure(f) => logger.info(
          "Sparse supplement table not found in " + pconfig.get(PipelineConfiguration.keyspaceName))
          false
      }
    }
    catch
    {
      case e: Exception =>
        logger.info("Sparse supplement table not found. Returning false after error " + e.getMessage)
        false
    }
  }


  @throws(classOf[IKodaMLException])
   def createDataTable(df: DataFrame, tableName: String):Boolean=
  {
    try
    {
      val df = getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName, "keyspace" -> keyspace))
        .load()
      logger.info(s"$keyspace.sparseSupplement exists")
      true
    }
    catch
    {
      case e: Exception =>
        logger.warn(e.getMessage)
        tableCreationProcess( tableName, df)
    }
  }

  @throws(classOf[IKodaMLException])
  private def tableCreationProcess( tableName: String, df: DataFrame): Boolean =
  {
    try
    {

      if(!isDuplicateFreeColumnNames(df))
        {
          throw new IKodaMLException("Duplicate columns in "+df.schema.fieldNames)
        }

      logger.info(s"tableCreationProcess: Creating $tableName table in ${pconfig.get(PipelineConfiguration.keyspaceName)}")
      logger.debug("tableCreationProcess: incoming  "+DataFrameUtils.showString(df))


      val newFieldNames: Seq[StructField] = df.schema.toSeq.map
      {
        sf => new StructField(cleanColumnName(sf.name), sf.dataType, sf.nullable)
      } ++ Seq(new StructField("timestamp", LongType, false))

      val allFields = makeSchema(newFieldNames)
      val schema: StructType = new StructType(allFields.toArray)
      val dataRDD = getSparkSession().sparkContext.emptyRDD[Row]
      val dfLowerCase = getSparkSession().createDataFrame(dataRDD, schema)
      logger.debug("tableCreationProcess: outgoing  "+DataFrameUtils.showString(df))
      reallyCreateTable(tableName, dfLowerCase)
    }
    catch
    {
      case e: Exception => throw new IKodaMLException(e.getMessage, e)
    }
  }


  private def reallyCreateTable(tableName: String, df: DataFrame): Boolean =
  {
    try
    {

      logger.debug("\nreallyCreateTable:\n" + DataFrameUtils.showString(df))

        df.createCassandraTable(
          keyspace,
          tableName,
          partitionKeyColumns = Some(Seq("uuid")),
          clusteringKeyColumns = Some(Seq("aa_label")))


      logger.info(
        "\n\n--------------------\nTable "+tableName+" created in  " + keyspace + "\n--------------------\n")
      true
    }
    catch
    {
      case e: Exception => logger.error(e.getMessage, e)
        false
    }
  }

  def makeSchema(newFieldNames: Seq[StructField]): Seq[StructField] =
  {

      Seq(StructField("uuid", StringType, true)) ++ newFieldNames


  }




}
