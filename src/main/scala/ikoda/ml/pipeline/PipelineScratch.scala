package ikoda.ml.pipeline

import grizzled.slf4j.Logging
import ikoda.ml.cassandra._
import ikoda.sparse.{ColumnHeadTuple, RDDLabeledPoint}
import ikoda.utilobjects.{DataFrameUtils, SimpleLog, SparkConfProviderWithStreaming, UtilFunctions}
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Try

trait PipelineScratch extends Logging with SimpleLog  with SparkConfProviderWithStreaming with UtilFunctions with PipelineMethods
{

}



