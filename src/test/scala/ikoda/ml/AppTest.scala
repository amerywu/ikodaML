package ikoda.ml

import ikoda.utilobjects.{ SparkConfProviderWithStreaming}
import org.junit.Assert._
import org.junit._

import scala.collection.mutable
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.{Properties, Random}
import scala.util.hashing.MurmurHash3._

@Test
class AppTest extends SparkConfProviderWithStreaming
{
  
  
  private def openFile(threshold: Double, path: String): Iterator[String] =
  {
    val t = threshold
    val lines: Iterator[String] = Source.fromFile(path).getLines
    val newLines = lines.filter(_ => Random.nextDouble() <= threshold)
    return newLines
    
  }
  
  private def writeFile(lines: Iterator[String], path: String): Unit =
  {
    val w = new java.io.FileWriter(new java.io.File(path))
    lines.foreach
    { s =>
      w.write(s + Properties.lineSeparator)
    }
    w.close
  }
  
  @Test
  def testOpenSources(): Unit =
  {
    try
    {
      val m:mutable.HashMap[String,String]=new mutable.HashMap
      

      
      assertTrue(true)
    }
    catch
    {
      case ex: Exception =>
      {
        println(s"There has been an Exception. Message is ${ex.getMessage} and ${ex}")
      }
    }
  }
  
  private def line2Data(line: String): List[Double] =
  {
    line
      .split("\\s+")
      .filter(_.length > 0)
      .map(_.toDouble)
      .toList
  }
  
  
  def reservoirSample[T: ClassTag](input: Iterator[T], numRows: Int): Array[T] =
  {
    val reservoir = new Array[T](numRows)
    // Put the first k elements in the reservoir.
    var i = 0
    while (i < numRows && input.hasNext)
    {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }
    
    if (i < numRows)
    {
      // If input size < k, trim the array size
      reservoir.take(i)
    }
    else
    {
      // If input size > k, continue the sampling process.
      while (input.hasNext)
      {
        val item = input.next
        val replacementIndex = Random.nextInt(i)
        if (replacementIndex < numRows)
        {
          reservoir(replacementIndex) = item
        }
        i += 1
      }
      reservoir
    }
  }
  
  
 
  

  def consistentFilter(s: String): Boolean = {
    val markLow = 0
    val markHigh = 4096
    val seed = 12345
  
    
    val hash = stringHash(s.split(" ")(0), seed) >>> 16
    hash >= markLow && hash < markHigh
  }
  
  
  
  
  //    @Test
  //    def testKO() = assertTrue(false)
  
}


