package ikoda.ml.cassandra

import scala.collection.JavaConverters._
import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.spark.connector.cql.CassandraConnector
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.utilobjects.SparkConfProviderWithStreaming
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

trait QueryExecutor extends Logging with SparkConfProviderWithStreaming
{



  @throws(classOf[IKodaMLException])
  protected def isException(rso:Try[ResultSet]): Unit =
  {

    rso.isSuccess match
    {
      case false => throw new IKodaMLException(rso.failed.get.getMessage, rso.failed.get)
      case true =>
    }
  }


  protected def getStringFromResultSet(r:Row,columnName:String):String =
  {
    val st=Try(r.getString(columnName))
    st.isSuccess match
      {
      case true => st.get
      case false =>
        logger.warn("getStringFromResultSet for "+columnName+" "+st.failed.get.getMessage)
        "Not Found"
    }
  }

  protected def debugRow(r:Row): String =
  {
    val sb:StringBuilder=new StringBuilder



    r.getColumnDefinitions.asList().asScala.foreach
    {
      e=> sb.append(e.getKeyspace+"\n")
        sb.append(e.getName +"\n")
        sb.append(e.getTable +"\n")
        sb.append(e.getType+"\n\n")
    }

    sb.toString()
  }

  protected def cleanColumnName(s:String): String =
  {
    val cleaner= s.replaceAll("[^A-Za-z0-9_-]","").toLowerCase()

    cleaner match
        {
        case x if(x.startsWith("_")) => "u"+x
        case x if(x.startsWith("-")) => "u"+x
        case _ => cleaner
      }


  }

  protected def isDuplicateFreeColumnNames(df:DataFrame): Boolean =
  {
    df.schema.fieldNames.size == df.schema.fieldNames.map(fn => fn.toLowerCase).toSet.size
  }


  protected def addColumnIfNotExists(incomingColumns:Set[String], tableName:String, keyspaceName:String): Unit =
  {
    try
    {
      val query = s"SELECT * FROM system_schema.columns WHERE keyspace_name = '$keyspaceName' AND table_name = '$tableName';"
      val resultt: Try[ResultSet] = executeQuery(query)
      if (resultt.isSuccess)
      {
        val itr = resultt.get.iterator()
        val extantCols: ArrayBuffer[String] = new ArrayBuffer[String]()
        while (itr.hasNext)
        {
          val row = itr.next()
          extantCols += row.getString("column_name")
        }
        val cleanExtantCols:Set[String]=extantCols.map(s => cleanColumnName(s)).toSet
        logger.debug("existing columns "+extantCols.mkString("||"))
        val cleanIncomingColumns=  incomingColumns.map(is => cleanColumnName(is)).toSet

        logger.debug("incomingColumns "+cleanIncomingColumns.mkString("<>"))
        val newColumns: String =cleanIncomingColumns.diff(cleanExtantCols).map(s => s + " text").toSet.mkString(",")
        if(newColumns.nonEmpty)
        {
          logger.debug("columns to Insert " + newColumns)
          val query: String = s"ALTER TABLE " + keyspaceName + ".\"" + tableName + "\" ADD (" + newColumns + ")"
          executeQuery(query)
        }
      }
    }
    catch
    {
      case e:Exception=> throw IKodaMLException(e.getMessage,e)
    }
  }





  protected def executeQuery(query:String): Try[ResultSet] =
  {
    try
    {
      //logger.debug("executeQuery: " + query)
      System.out.print(".")

      val rstry:Try[ResultSet]=Try(CassandraConnector(getSparkSession.sparkContext.getConf).withSessionDo
      {
        session =>
          session.execute(query)

      })
      rstry match
        {
        case Success(rs)=>rstry
        case Failure(f) => logger.error("\n\nQUERY THREW EXCEPTION:\n"+query+"\n"+f.getMessage,f)
          rstry
      }
    }
    catch
      {
        case e:Exception => logger.error("\n\nQUERY THREW EXCEPTION:\n"+query+"\n"+e.getMessage,e)
          Failure(e)
      }
  }
}
