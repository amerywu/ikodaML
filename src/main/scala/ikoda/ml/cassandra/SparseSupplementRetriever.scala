package ikoda.ml.cassandra

import com.datastax.driver.core.ResultSet
import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.ml.caseclasses.{LemmatizedSentence, SentenceSupplementData}
import ikoda.ml.pipeline.PipelineConfiguration
import ikoda.sparse.ColumnHeadTuple
import ikoda.utilobjects.SparkConfProviderWithStreaming
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Loads dense supplementary data to a DataFrame
  * @param pconfig
  */
class SparseSupplementRetriever(pconfig:PipelineConfiguration) extends Logging  with QueryExecutor
{


  val keyspace=pconfig.get(PipelineConfiguration.keyspaceName)



  private def generateLemmatizedSentenceQuery(uuidSeq:Seq[LemmatizedSentence], keyspace:String):String=
  {
    val sb:StringBuilder=new StringBuilder
    sb.append("select * from  ")
    sb.append(keyspace)
    sb.append(".\"sparseSupplement\"  where uuid in(")
    val itr=uuidSeq.iterator
    while(itr.hasNext)
    {
      sb.append("'")
      sb.append(itr.next.sparseRow._3)
      sb.append("'")
      if(itr.hasNext)
      {
        sb.append(", ")
      }
    }
    sb.append(") allow filtering;")

    logger.debug(sb)
    sb.toString()

  }

  private def matchLemmatizedSentence(lsSeq:Seq[LemmatizedSentence],uuid:String):Option[LemmatizedSentence]=
  {
    lsSeq.find(ls => ls.sparseRow._3 == uuid)
  }




  @throws(classOf[IKodaMLException])
  def getLemmatizedSentences(uuidSeq:Seq[LemmatizedSentence]): Option[Seq[LemmatizedSentence]] =
  {
    try
    {
      val query=generateLemmatizedSentenceQuery(uuidSeq,keyspace)
      val rst=executeQuery(query)
      rst.isSuccess match
      {
        case true =>
          val lsOutSeq:ArrayBuffer[LemmatizedSentence]= new ArrayBuffer[LemmatizedSentence]
          val rs:ResultSet=rst.get
          rs.getAvailableWithoutFetching > 0 match
          {
            case true =>
              val iter = rs.iterator
              while (iter.hasNext) {
                if ( !rs.isFullyFetched) {rs.fetchMoreResults}
                val row = iter.next

                val u = getStringFromResultSet(row, "uuid")
                val l = getStringFromResultSet(row, "aa_label")
                val r = getStringFromResultSet(row, "rawsentence")
                val lm = getStringFromResultSet(row, "lemmatizedsentence")

                val label = l.endsWith("-") match {
                  case true => l.substring(0, l.length - 1)
                  case false => l
                }

                val extantSentenceo=matchLemmatizedSentence(uuidSeq,u)
                if(extantSentenceo.isDefined) {
                  val extant=extantSentenceo.get
                  lsOutSeq += LemmatizedSentence(extant.sparseRow, r, lm, extant.clusterId,extant.target,l,extant.term1,extant.term1Value,extant.term2,extant.term2Value,extant.subset)
                }



                val ssd = SentenceSupplementData(
                  u,
                  label,
                  r,
                  lm)
              }
              Some(lsOutSeq)
            case false =>
              logger.warn("SparseSupplementRetriever: No data found for sentences in "+uuidSeq.mkString(", "))
              None
              }

        case false =>
          logger.warn("SparseSupplementRetriever: getLemmatizedSentence "+rst.failed.get.getMessage,rst.failed.get)
          None
      }
    }
    catch
      {
        case e: Exception => throw IKodaMLException(e.getMessage, e)
      }
  }



  @throws(classOf[IKodaMLException])
  def loadSparseSupplement(): DataFrame =
  {
    try
    {

        try
        {
          logger.debug(s"loading ${pconfig.get(PipelineConfiguration.keyspaceName)}.sparseSupplement")
          getSparkSession()
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> "sparseSupplement", "keyspace" -> pconfig.get(PipelineConfiguration.keyspaceName)))
            .load()
        }
        catch
        {
          case e: Exception => throw IKodaMLException(e.getMessage, e)
        }


    }
    catch
    {

      case e:Exception=>
        logger.error(e.getMessage,e)
        throw  IKodaMLException(e.getMessage,e)
    }
  }



}
