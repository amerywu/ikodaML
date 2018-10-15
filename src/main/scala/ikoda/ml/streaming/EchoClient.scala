package ikoda.ml.streaming



import java.io._
import java.net._

import grizzled.slf4j.Logging

import scala.io._



object EchoClient extends Logging{

  
  

  def main(args : Array[String]) : Unit = {
  

  while(true)
  {
    logger.info ("1")
    val s = new Socket(InetAddress.getByName("192.168.0.13"), 9999)
    logger.info ("2")
    lazy val in = new BufferedSource(s.getInputStream()).getLines()
    logger.info ("3")
    val out = new PrintStream(s.getOutputStream())
  
    out.println("thanks")
    out.flush()
    val received=in.next()
    println("Received: " + received)
    logger.info ("4 "+received)
  
    s.close()
    Thread.sleep(5000)
  
  }
  }
}
