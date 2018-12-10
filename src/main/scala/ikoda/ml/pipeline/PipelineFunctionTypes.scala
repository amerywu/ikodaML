package ikoda.ml.pipeline

import ikoda.sparse.RDDLabeledPoint
import org.apache.spark.sql.DataFrame

trait PipelineFunctionTypes {
  type FTDataReductionProcess =
    (PipelineConfiguration => (Option[RDDLabeledPoint] => Option[RDDLabeledPoint]))

  type FTDataReductionProcessDataFrame = (PipelineConfiguration => (Option[DataFrame] => Option[DataFrame]))
}
