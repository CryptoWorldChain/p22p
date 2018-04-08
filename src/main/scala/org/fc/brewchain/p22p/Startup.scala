package org.fc.brewchain.p22p

import onight.osgi.annotation.NActorProvider
import com.google.protobuf.Message
import onight.oapi.scala.commons.SessionModules
import org.apache.felix.ipojo.annotations.Validate
import org.apache.felix.ipojo.annotations.Invalidate
import org.fc.brewchain.bcapi.URLHelper
import org.fc.brewchain.p22p.node.Networks
import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.core.MessageSender
import onight.tfw.otransio.api.NonePackSender
import onight.oapi.scala.traits.OLog
import java.net.URL

@NActorProvider
object Startup extends SessionModules[Message] {

  @Validate
  def init() {

    //    System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.url");
    log.info("startup:");
    new Thread(new BackgroundLoader()).start()

    log.debug("bdb_prodiver==" + Daos.bdbprovider);
    log.info("tasks inited....[OK]");
  }

  @Invalidate
  def destory() {

  }

}

class BackgroundLoader() extends Runnable with OLog {
  def run() = {
    URLHelper.init();
    while (!Daos.isDbReady() || MessageSender.sockSender.isInstanceOf[NonePackSender]) {
      log.debug("Daos Or sockSender Not Ready..")
      Thread.sleep(1000);
    }

    val networks = Daos.props.get("org.bc.pzp.networks", "raft").split(",").toList
    log.debug("networks:" + networks)
    networks.map { x =>
      val net = new Network(x.trim(), Daos.props.get("org.bc.pzp.networks." + x.trim() + ".nodelist", "tcp://127.0.0.1:5100"));
      Networks.netsByID.put(net.netid, net)
      net.initNode();
      net.startup()
    }
  }
}