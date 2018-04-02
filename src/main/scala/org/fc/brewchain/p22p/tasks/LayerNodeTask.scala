package org.fc.brewchain.p22p.tasks

import java.util.concurrent.TimeUnit
import onight.oapi.scala.traits.OLog
import org.fc.brewchain.p22p.node.NodeInstance
import org.fc.brewchain.p22p.pbgens.P22P.PMNodeInfo
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteNodeIdx
import java.math.BigInteger
import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.p22p.core.MessageSender
import org.fc.brewchain.p22p.pbgens.P22P.PSJoin
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CallBack
import org.fc.brewchain.p22p.pbgens.P22P.PRetJoin
import java.util.concurrent.ScheduledFuture
import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.node.Networks
import org.slf4j.MDC
import org.fc.brewchain.p22p.utils.Config

//投票决定当前的节点
object LayerNodeTask extends OLog with Runnable {

  def initTask() {
    new Thread(this).start();
  }

  def run() = {

    while (!NodeInstance.isReady()) {
      Thread.sleep(1000);
    }
    
    log.debug("Starting Node Tasks");
    //add myself
    Networks.instance.addPendingNode(NodeInstance.root())
    
    Scheduler.scheduleWithFixedDelay(JoinNetwork, 5, 60, TimeUnit.SECONDS)
    Scheduler.scheduleWithFixedDelay(CheckingHealthy, 10, Config.TICK_CHECK_HEALTHY, TimeUnit.SECONDS)
    Scheduler.scheduleWithFixedDelay(VoteNodeMap, 10, Config.TICK_VOTE_MAP, TimeUnit.SECONDS)

  }
//  lazy val currPMNodeInfo = PMNodeInfo.newBuilder().setAddress(NodeInstance.root.address) //
//    .setNodeName(NodeInstance.root.name).setPort(NodeInstance.root.port)
//    .setProtocol("tcp")
//    .setPubKey(NodeInstance.root.pub_key).setStartupTime(NodeInstance.root.startup_time).setTryNodeIdx(NodeInstance.root.try_node_idx)
//    .setNodeIdx(NodeInstance.root.node_idx);

}