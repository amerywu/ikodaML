package ikoda.ml.cassandra

import scala.util.Try

/**
  * The configuration parameters for a keyspace. Loaded from keyspaces.conf
  *
  * @param flusho true if this keyspace will be flushed
  * @param trimo true if low frequency columns should be removed during fush
  * @param supplemento true if supplementary dense data expected
  * @param hdfso true if directories are on Hadoop
  * @param truncateoverwriteo true if keyspace data is to be overwritten with each batch
  * @param archiveo true if incoming files shou,d be archived
  * @param flushthresholdo staging keyspace rowcount that triggers flush
  * @param rfo
  * @param uidinsparseo true if sparse data shares a UID with supplementary dense data
  * @param diro the staging directory for sparse data
  * @param persisteddiro the destination for files already persisted to Cassandra
  * @param errordiro the destination for files that failed during persistence process
  * @param supplementdiro the staging directory for supplementary dense data
  * @param supplementpersisteddiro the destination for supplementary files that failed during persistence process
  * @param supplementerrordiro the destination for supplementary files that failed during persistence process
  * @param archivediro
  * @param supplementsuffixo the filename suffix that identifies a supplementary data file
  * @param uuido the uuid column name
  * @param permittedProportionOfDuplicateso the permitted proportion of duplicate rows that can be persisted.
  */
case class CassandraKeyspaceConfiguration(
                                           flusho:Try[Boolean],
                                           trimo:Try[Boolean],
                                           supplemento:Try[Boolean],
                                           hdfso:Try[Boolean],
                                           truncateoverwriteo:Try[Boolean],
                                           archiveo:Try[Boolean],
                                           flushthresholdo:Try[Int],
                                           rfo:Try[Int],
                                           uidinsparseo:Try[String],
                                           diro:Try[String],
                                           persisteddiro:Try[String],
                                           errordiro:Try[String],
                                           supplementdiro:Try[String],
                                           supplementpersisteddiro:Try[String],
                                           supplementerrordiro:Try[String],
                                           archivediro:Try[String],
                                           supplementsuffixo:Try[String],
                                           uuido:Try[String],
                                           permittedProportionOfDuplicateso:Try[Double]
                                         )
{
  lazy val flush:Boolean = flusho.getOrElse(false)
  lazy val trim:Boolean = trimo.getOrElse(true)
  lazy val supplement:Boolean = supplemento.getOrElse(false)
  lazy val hdfs:Boolean = hdfso.getOrElse(true)
  lazy val truncateoverwrite:Boolean = truncateoverwriteo.getOrElse(false)
  lazy val archive:Boolean = archiveo.getOrElse(false)
  lazy val flushthreshold:Int=flushthresholdo.getOrElse(100000)
  lazy val rf:Int = rfo.getOrElse(1)
  lazy val uidinsparse:String=uidinsparseo.getOrElse("a_uid")
  lazy val dir:String = diro.getOrElse( "/ikoda/streamedData/rawSparseNLP")
  lazy val persistedDir:String = persisteddiro.getOrElse("/ikoda/streamedData/rawSparseNLPPersisted")
  lazy val errordir:String = errordiro.getOrElse("/ikoda/streamedData/rawSparseNLPFailed")
  lazy val supplementdir:String = supplementdiro.getOrElse( "/ikoda/streamedData/rawSparseNLPSupplement")
  lazy val supplementpersisteddir:String = supplementpersisteddiro.getOrElse("/ikoda/streamedData/rawSparseNLPSupplementPersisted")
  lazy val supplementerrordir:String = supplementerrordiro.getOrElse("/ikoda/streamedData/rawSparseNLPSupplementFailed")
  lazy val archivedir:String=archivediro.getOrElse("/ikoda/streamedData/rawSparseNLPArchive")
  lazy val supplementsuffix:String=supplementsuffixo.getOrElse("supplement")
  lazy val uuid:String=uuido.getOrElse("")
  lazy val permittedProportionOfDuplicates:Double=permittedProportionOfDuplicateso.getOrElse(1.0)

  override def toString: String = s"\nflush ${flush}\n trim ${trim}\nsupplement ${supplement}\ntruncateoverwrite ${truncateoverwrite}\nflushthreshold ${flushthreshold}\nrf ${rf}\n"+
    s"uidinsparse ${uidinsparse}\n dir ${dir}\npersistedDir ${persistedDir}\nerrordir ${errordir}\nsupplementdir ${supplementdir}\nsupplementarchivedir ${supplementpersisteddir}\n"+
    s"supplementerrordir ${supplementerrordir}\npermittedProportionOfDuplicates ${permittedProportionOfDuplicates}\n\n"



}
