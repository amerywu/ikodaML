package ikoda.ml.cassandra


import java.util.UUID

import scala.collection.JavaConverters._
import com.datastax.driver.core.{ResultSet, Row}
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.{ColumnHeadTuple, RDDLabeledPoint}
import ikoda.utilobjects.{SparkConfProviderWithStreaming, UtilFunctions}
import ikoda.utils.TicToc
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Loads data from Cassandra into an ikoda.sparse.RDDLabeledPoint instance
  *
  * @param pconfig Configuration data specifying keyspace name
  */
class SparseDataToRDDLabeledPoint(pconfig: PipelineConfiguration) extends Logging with UtilFunctions with
  QueryExecutor {


  val sqlContext: SQLContext = getSparkSession().sqlContext

  import sqlContext.implicits._

  val keyspace: String = pconfig.get(PipelineConfiguration.keyspaceName)

  //select indices  from fjd201808bysentence."sparseData" where indices contains 321 and indices contains 747 and label = 3.0 allow filtering;
  @throws(classOf[IKodaMLException])
  def loadFromCassandra: RDDLabeledPoint = {
    try {

      val tt: TicToc = new TicToc
      logger.info(tt.tic(s"Loading data from $keyspace ....."))
      logger.info(tt.tic(s"loadTargets"))
      val datadict: Map[String, Double] = loadTargets()
      logger.info(s"Loaded ${datadict.size} targets.")
      logger.info(tt.toc(s"loadTargets"))
      logger.info(tt.tic(s"loadColumns"))
      val columns: mutable.ListMap[Int, ColumnHeadTuple] = loadColumns()
      logger.info(s"Loaded ${columns.size} columns.")
      logger.info(tt.toc(s"loadColumns"))
      logger.info(tt.tic(s"loadFromCassandra loadData"))


      val labeledPointRdd: Dataset[(LabeledPoint, Int, String)] = loadDataAsLabeledPoints(columns.size).map {
        r =>
          if (null == r._3 || r._3.isEmpty) {
            System.out.println("loadFromCassandra Creating new uuid")
            (r._1, r._2, UUID.randomUUID().toString)
          }
          else {

            (r._1, r._2, r._3)
          }
      }
      logger.info(tt.toc(s"loadFromCassandra loadData"))
      logger.info(tt.tic(s"loadLibSvmFromHelper"))

      val sparseOut = getOrThrow(RDDLabeledPoint.loadLibSvmFromHelper(labeledPointRdd, columns, datadict, keyspace))
      logger.info(tt.toc(s"loadLibSvmFromHelper"))
      logger.info(sparseOut.show(5))
      logger.info(tt.toc(s"Loading data from $keyspace ....."))
      logger.info(s"Loaded ${sparseOut.info}")

      sparseOut
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def loadFromCassandra(query: String): Option[RDDLabeledPoint] = {
    try {

      val tt: TicToc = new TicToc
      logger.info(tt.tic(s"Loading data from $keyspace ....."))

      logger.info(tt.tic(s"loadColumns"))
      val columns: mutable.ListMap[Int, ColumnHeadTuple] = loadColumns()
      logger.info(s"Loaded ${columns.size} columns.")
      logger.info(tt.toc(s"loadColumns"))

      logger.info(tt.tic(s"loadFromCassandra loadData"))

      val datao = loadDataAsLabeledPoints(query, columns.size)

      datao.isDefined match {

        case true =>

          val labeledPointSeq: Seq[(LabeledPoint, Int, String)] = datao.get.map {
            r =>
              if (null == r._3 || r._3.isEmpty) {
                logger.warn("loadFromCassandra Creating new uuid")
                (r._1, r._2, UUID.randomUUID().toString)
              }
              else {

                (r._1, r._2, r._3)
              }
          }
          logger.info(tt.toc(s"loadFromCassandra loadData"))

          logger.info(tt.tic(s"loadTargets"))
          val datadict: Map[String, Double] = loadTargets()
          logger.info(s"Loaded ${datadict.size} targets.")
          logger.info(tt.toc(s"loadTargets"))

          logger.info(tt.tic(s"loadLibSvmFromHelper"))


          val ss = spark()
          import ss.implicits._
          val labeledPointDs = spark.sparkContext.parallelize(labeledPointSeq).toDS()


          val tryToLoad:Try[RDDLabeledPoint] = RDDLabeledPoint.loadLibSvmFromHelper(labeledPointDs, columns, datadict, keyspace)
          logger.info(tt.toc(s"loadLibSvmFromHelper"))
          logger.info(tt.toc(s"Loading data from $keyspace ....."))

          tryToLoad.isSuccess match
            {
            case true => Some(tryToLoad.get)
            case false=> None
          }


        case false=> None
      }
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def loadSchemaFromCassandra: RDDLabeledPoint = {
    try {
      logger.info(s"loadSchemaFromCassandra Loading schema from $keyspace .....")
      val datadict: Map[String, Double] = loadTargets()
      logger.info(s"loadSchemaFromCassandra Loaded targets from $keyspace .....")
      val columns: mutable.ListMap[Int, ColumnHeadTuple] = loadColumns()
      logger.info(s"loadSchemaFromCassandra Loaded columns from $keyspace .....")
      val sparse0: RDDLabeledPoint = new RDDLabeledPoint
      getOrThrow(RDDLabeledPoint.loadLibSvmSchemaFromHelper1(columns, datadict, keyspace))


    }
    catch {
      case e: Exception =>
        logger.warn(e.getMessage, e)
        throw IKodaMLException(e.getMessage, e)
    }
  }

  @throws(classOf[IKodaMLException])
  private def loadTargets(): Map[String, Double] = {
    try {
      getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "targetMap", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .load().collect().map {
        r => (r.getAs[String]("targetName"), r.getAs[Double]("targetIndex"))
      }.toMap
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def loadColumns(): mutable.ListMap[Int, ColumnHeadTuple] = {
    try {
      val map = getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "columnMap", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .load().collect().map {
        r => (r.getAs[String]("columnName"), r.getAs[Int]("columnIndex"))
      }.toSeq.map {
        col => (col._2, ColumnHeadTuple(col._2, col._1))
      }

      mutable.ListMap(map: _*)
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def loadOldNewIdMap(): Map[Double, String] = {
    try {
      logger.debug("Loading oldNewUidMap from " + pconfig.get(PipelineConfiguration.keyspaceName))
      getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "oldnewuid", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .load().collect().map {
        r => r.getAs[Double]("oldid") -> r.getAs[String]("newid")
      }.toMap
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def loadDataAsLabeledPoints(colCount: Int): Dataset[(LabeledPoint, Int, String)] = {
    try {



      val dataRdd: Dataset[(LabeledPoint, Int, String)] = getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "sparseData", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
        .load().map {
        r =>
          ( r.getAs[String]("uuid"),
            r.getAs[Int]("hashcode"),
            r.getAs[List[Int]]("indices"),
            r.getAs[List[Double]]("values"),
            r.getAs[Double]("label")
          )
      }.map {
        rowAsSeq =>

          val lpt:Try[LabeledPoint] = Try(new LabeledPoint(rowAsSeq._5, org.apache.spark.ml.linalg.Vectors.sparse(colCount, rowAsSeq._3.toArray, rowAsSeq._4.toArray)))
          lpt.isSuccess match {
            case true => (lpt.get, lpt.get.hashCode(), rowAsSeq._1)
            case false =>
              val map=SortedMap((rowAsSeq._3 zip rowAsSeq._4):_*)
              val lp= new LabeledPoint(rowAsSeq._5, org.apache.spark.ml.linalg.Vectors.sparse(colCount, map.keySet.toArray, map.values.toArray))
              (lp, lp.hashCode(), rowAsSeq._1)

          }

      }
      var l: ArrayBuffer[Long] = new ArrayBuffer[Long]()
      dataRdd
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def loadDataAsDataFrame(): DataFrame= {
    try {

      getSparkSession()
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "sparseData", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))

        .load()

    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  private def loadDataAsLabeledPoints(query: String, colCount: Int): Option[Seq[(LabeledPoint, Int, String)]] = {
    try {
      val rst: Try[ResultSet] = executeQuery(query)
      rst.isSuccess match {
        case true =>
          val rs: ResultSet = rst.get
          val rsList: mutable.Buffer[Row] = rs.all.asScala
          val returnSeq = rsList.map {
            row =>

              val uuid = row.getString("uuid")
              val hc = row.getLong("hashcode")
              val indices: Seq[Int] = row.getList("indices", classOf[java.lang.Integer]).asScala.map(e=> e.toInt)
              val values: Seq[Double] = row.getList("values", classOf[java.lang.Double]).asScala.map(e=>e.toDouble)
              val label = row.getDouble("label")

              val lp = new LabeledPoint(label, org.apache.spark.ml.linalg.Vectors.sparse(colCount, indices.toArray, values.toArray))
              (lp, lp.hashCode(), uuid)

          }
          Some(returnSeq)
        case false => None
      }
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


}




