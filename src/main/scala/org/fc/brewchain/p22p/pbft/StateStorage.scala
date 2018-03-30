package org.fc.brewchain.p22p.pbft

import java.util.concurrent.atomic.AtomicInteger
import org.fc.brewchain.p22p.Daos
import org.apache.commons.lang3.StringUtils
import onight.oapi.scala.traits.OLog

object StateStorage extends OLog {

  val N = new AtomicInteger(0)
  val V = new AtomicInteger(0)
  val VIEW_ID_PROP = "org.bc.pbft.view.state"

  def init() {
  }
  def nextN(): Int = {
    N.incrementAndGet()
  }
  def curV(): Int = {
    V.get
  }
  def nextV(): Int = {
    this.synchronized {
      val vid = V.incrementAndGet();
//      Daos.odb.put(SEQ_ID_PROP, vid.asInstanceOf[String])
      vid
    }
  }
}