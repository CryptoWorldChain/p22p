package org.fc.brewchain.p22p.tasks

import java.util.concurrent.TimeUnit
import onight.oapi.scala.traits.OLog
import org.fc.brewchain.p22p.node.NodeInstance
import org.fc.brewchain.p22p.pbgens.P22P.PMNodeInfo
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteNodeIdx
import java.math.BigInteger
import org.fc.brewchain.p22p.node.PNode
import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.p22p.core.MessageSender
import org.fc.brewchain.p22p.pbgens.P22P.PSJoin
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CallBack
import org.fc.brewchain.p22p.pbgens.P22P.PRetJoin
import java.net.URL
import org.apache.felix.framework.URLHandlers
import org.fc.brewchain.bcapi.URLHelper
import org.fc.brewchain.p22p.action.PMNodeHelper
import org.fc.brewchain.bcapi.crypto.BitMap
import org.fc.brewchain.p22p.pbgens.P22P.PVBase
import org.fc.brewchain.p22p.pbgens.P22P.PBFTStage
import org.fc.brewchain.p22p.node.Networks
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import org.fc.brewchain.p22p.pbgens.P22P.PSNodeInfo
import org.fc.brewchain.p22p.pbgens.P22P.PRetNodeInfo

import scala.collection.JavaConversions._
//投票决定当前的节点
object CheckingHealthy extends SRunner {
  def getName() = "CheckingHealthy"
  def runOnce() = {
    Networks.instance.pendingNodes.filter { _.bcuid != NodeInstance.root().bcuid }.map { n =>
      val pack = PSNodeInfo.newBuilder();
      log.debug("checking Health to @" + n.bcuid + ",uri=" + n.uri())
      MessageSender.sendMessage("HBTPZP", pack.build(), n, new CallBack[FramePacket] {
        def onSuccess(fp: FramePacket) = {
          log.debug("send HBTPZP success:to " + n.uri + ",body=" + fp.getBody)
          val retpack = PRetNodeInfo.newBuilder().mergeFrom(fp.getBody);
//          log.debug("get nodes:" + retpack);
          if (retpack.getCurrent == null) {
            log.debug("Node EROR NotFOUND:" + retpack);
            Networks.instance.removePendingNode(n);
          } else if (!StringUtils.equals(retpack.getCurrent.getBcuid, n.bcuid)) {
            log.debug("Node EROR BCUID Not Equal:" + retpack.getCurrent.getBcuid + ",n=" + n.bcuid);
            Networks.instance.removePendingNode(n);
          } else {
            log.debug("get nodes:pendingcount=" + retpack.getPnodesCount);
            retpack.getPnodesList.map { pn =>
              Networks.instance.addPendingNode(fromPMNode(pn));
            }
          }
        }
        def onFailed(e: java.lang.Exception, fp: FramePacket) {
          log.debug("send HBTPZP ERROR " + n.uri + ",e=" + e.getMessage, e)
          Networks.instance.removePendingNode(n);
        }
      });
    }

  }
  //Scheduler.scheduleWithFixedDelay(new Runnable, initialDelay, delay, unit)
  def main(args: Array[String]): Unit = {
    URLHelper.init()
    //System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.bcapi.url");
    println(new URL("tcp://127.0.0.1:5100").getHost);
  }
}