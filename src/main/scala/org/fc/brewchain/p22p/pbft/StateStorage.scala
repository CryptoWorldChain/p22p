package org.fc.brewchain.p22p.pbft

import java.util.concurrent.atomic.AtomicInteger
import org.fc.brewchain.p22p.Daos
import org.apache.commons.lang3.StringUtils
import onight.oapi.scala.traits.OLog

object StateStorage extends OLog {

  val N = new AtomicInteger(0)
  val V = new AtomicInteger(0)
  val VIEW_ID_PROP = "org.bc.pbft.view.id"
  val SEQ_ID_PROP = "org.bc.pbft.view.seq"

  def init() {
    var v = Daos.odb.get(VIEW_ID_PROP);
    val vid = if (v == null || v.get() == null || !StringUtils.isNumeric(v.get.getValue)) {
      0
    } else {
      log.debug("Load View ID  from DB=" + v.get.getValue)
      Integer.parseInt(v.get.getValue);
    }
    V.set(vid)

    var n = Daos.odb.get(SEQ_ID_PROP);
    val nid = if (v == null || v.get() == null || !StringUtils.isNumeric(v.get.getValue)) {
      0
    } else {
      log.debug("Load View ID  from DB=" + v.get.getValue)
      Integer.parseInt(v.get.getValue);
    }
    N.set(nid)
    log.debug("Init View(v,n):" + (vid, nid))
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
      Daos.odb.put(SEQ_ID_PROP, N.get.asInstanceOf[String])
      Daos.odb.put(SEQ_ID_PROP, vid.asInstanceOf[String])
      vid
    }
  }
}