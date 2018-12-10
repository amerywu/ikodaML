package ikoda.ml.cassandra

import grizzled.slf4j.Logging

object SparseModelReader extends Logging  with QueryExecutor{

  def listKeyspaces(): Unit=
  {
    logger.info(executeQuery("SELECT * FROM system_schema.keyspaces;"))
  }

}
