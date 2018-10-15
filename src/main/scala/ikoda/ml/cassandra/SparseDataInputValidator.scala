package ikoda.ml.cassandra

import grizzled.slf4j.Logging
import ikoda.IKodaMLException
import ikoda.sparse.RDDLabeledPoint
import jnr.ffi.annotations.Synchronized
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Validates data prior to input. Throws Exception if data is invalid.
  */
object SparseDataInputValidator  extends Logging{




  private def doValidation(sparse0: RDDLabeledPoint,lpArrayReAligned: Array[(LabeledPoint, Int, String)],extantColumns: Map[String, Int]): Boolean =
  {
    try
    {
      val validationSet: Array[((LabeledPoint, Int, String))] = sparse0.rowCountEstimate match {
        case x if (x > 5) => sparse0.sparseData.take(5)
        case _ => sparse0.sparseData.collect()

      }

      validationSet.foreach {
        r =>
          if (isValidUUID(Option(r._3))) {
            val uuidInSparse = r._3
            val rowRealignedo: Option[Tuple3[LabeledPoint, Int, String]] = lpArrayReAligned.find(e => e._3 == uuidInSparse)
            if (rowRealignedo.isDefined) {
              val rowRealigned: Tuple3[LabeledPoint, Int, String] = rowRealignedo.get
              val colNamesRealignedSet: Set[String] = colNamesRealigned(rowRealigned, extantColumns)
              val colNamesInSparseSet: Set[String] = colNamesInSparse(sparse0, r._1)
              require(colNamesInSparseSet.diff(colNamesRealignedSet).size == 0)
            }

          }
          else {
            logger.warn("No UUID against which to validate row ")
          }
      }
      logger.info("|             Passed data validation              |")
      true
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  @throws(classOf[IKodaMLException])
  def validateSparseData(sparse0: RDDLabeledPoint,lpArrayReAligned: Array[(LabeledPoint, Int, String)],extantColumns: Map[String, Int]): Boolean = {

    synchronized {

      try {
        logger.info("Validating Data Before Inserting")
        sparse0.rowCountEstimate > 100 match {
          case true =>
            val subseto: Option[RDDLabeledPoint] = RDDLabeledPoint.randomSubset(sparse0,0.05)
            subseto.isDefined match {
              case false =>
                throw new IKodaMLException("Could not obtain a subset from sparse")
                false
              case true =>
                doValidation(subseto.get,lpArrayReAligned,extantColumns)



            }

          case false => logger.warn("\n\nTiny dataset. Cannot validate\n\n")
            true
        }
      }
      catch {
        case e: Exception => throw IKodaMLException(e.getMessage, e)
      }
    }
  }

  private def colNamesInSparse(sparse0:RDDLabeledPoint,lp:LabeledPoint): Set[String] =
  {
    try
    {
      lp.features.toSparse.indices.map
      {
        idx => sparse0.getColumnName(idx)
      }.toSet
    }
    catch {
      case e: Exception => throw IKodaMLException(e.getMessage, e)
    }
  }


  private def colNamesRealigned(rowRealigned: Tuple3[LabeledPoint, Int, String],extantColumns: Map[String, Int] ): Set[String] =
  {
    try
    {
      val invertedMap:Map[Int,String]=extantColumns.map(_.swap)
      rowRealigned._1.features.toSparse.indices.length > 0 match
      {
        case false => throw IKodaMLException(s"Realigned ${rowRealigned._3} has no valid columns")
        case true => rowRealigned._1.features.toSparse.indices.map
          {
            idx =>
              val colNameo:Option[String]=invertedMap.get(idx)
              colNameo.isDefined match
              {
                case false => throw IKodaMLException(s"Realigned${rowRealigned._3} has no valid column with idx $idx")
                case true => colNameo.get
              }
          }.toSet
      }
    }
    catch
      {
        case e: Exception => throw IKodaMLException(e.getMessage, e)

      }
  }



  private def isValidUUID(uidvalo: Option[String]): Boolean = {
    uidvalo match {
      case x if (!x.isDefined) => false
      case x if (x.get.isEmpty) => false
      case x if (x.get.length < 12) => false
      case _ => true
    }
  }


}
