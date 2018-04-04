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
import com.google.protobuf.ByteString
import org.fc.brewchain.p22p.pbft.StateStorage
import org.fc.brewchain.p22p.pbgens.P22P.PVType
import onight.tfw.outils.serialize.UUIDGenerator
import org.fc.brewchain.p22p.utils.Config
import org.fc.brewchain.p22p.pbft.VoteQueue

//投票决定当前的节点
object VoteNodeMap extends SRunner {
  def getName() = "VoteNodeMap"
  def runOnce() = {
    log.debug("VoteNodeMap :Run----Try to Vote Node Maps");
    val oldThreadName = Thread.currentThread().getName + ""
    try {

      Thread.currentThread().setName("VoteNodeMap");
      log.info("CurrentPNodes:PendingSize=" + Networks.instance.pendingNodes.size + ",DirectNodeSize=" + Networks.instance.directNodes.size);
      val vbase = PVBase.newBuilder();

      vbase.setState(PBFTStage.PENDING_SEND)
      vbase.setMType(PVType.NETWORK_IDX)
      var pendingbits = BigInt(1)
      //init. start to vote.
      if (JoinNetwork.pendingJoinNodes.size() / 2 > Networks.instance.onlineMap.size || JoinNetwork.pendingJoinNodes.size() < 1) {
        log.info("cannot vote for pendingJoinNodes Size bigger than online half:PendJoin=" +
          JoinNetwork.pendingJoinNodes.size() + ": Online=" + Networks.instance.onlineMap.size)
        //for fast load
      } else if (StateStorage.nextV(vbase) > 0) {
        vbase.setMessageUid(UUIDGenerator.generate())
        vbase.setOriginBcuid(NodeInstance.root().bcuid)
        vbase.setFromBcuid(NodeInstance.root.bcuid);
        vbase.setLastUpdateTime(System.currentTimeMillis())
        val vbody = PBVoteNodeIdx.newBuilder();
        var bits = Networks.instance.node_bits;
        pendingbits = BigInt(0)

        Networks.instance.pendingNodes.map(n =>
          //          if (Networks.instance.onlineMap.contains(n.bcuid)) {
          if (bits.testBit(n.try_node_idx)) {
            log.debug("error in try_node_idx @n=" + n.name + ",try=" + n.try_node_idx + ",bits=" + bits);
          } else { //no pub keys
            pendingbits = pendingbits.setBit(n.try_node_idx);
            vbody.addPendingNodes(toPMNode(n));
          } //          }
          )

        vbody.setPendingBitsEnc(BitMap.hexToMapping(pendingbits))
        vbody.setNodeBitsEnc(BitMap.hexToMapping(Networks.instance.node_bits))
        vbase.setContents(toByteSting(vbody))
        //      vbase.addVoteContents(Any.pack(vbody.build()))
        //      if (Networks.instance.node_bits.bitCount <= 0) {
        //        log.debug("networks has not directnode!")
        log.info("vote -- Nodes:" + vbody.getNodeBitsEnc + ",pendings=" + vbody.getPendingBitsEnc);
        vbase.setV(vbase.getV);
        vbase.setN(Networks.instance.pendingNodes.size + Networks.instance.directNodes.size);

        log.info("broadcast Vote Message:V=" + vbase.getV + ",N=" + vbase.getN + ",from=" + vbase.getFromBcuid
          + ",SN=" + vbase.getStoreNum + ",VC=" + vbase.getViewCounter + ",messageid=" + vbase.getMessageUid)
        val vbuild = vbase.build();
        //        Networks.wallMessage("VOTPZP", vbuild);
        VoteQueue.appendInQ(vbase.setState(PBFTStage.PENDING_SEND).build())
      }
      //      }
      //    NodeInstance.forwardMessage("VOTPZP", vbody.build());
      //vbody.setNodeBitsEnc(bits.toString(16));
      log.debug("Run-----[Sleep]"); //
      val sleepTime = if (pendingbits.bitCount > 0) {
        (Config.MAX_VOTE_SLEEP_MS - Config.MIN_VOTE_SLEEP_MS) + Config.MIN_VOTE_SLEEP_MS
      } else {
        (Config.MAX_VOTE_SLEEP_MS - Config.MIN_VOTE_SLEEP_MS) + Config.MIN_VOTE_WITH_NOCHANGE_SLEEP_MS
      }
      this.synchronized {
        this.wait((Math.random() * sleepTime).asInstanceOf[Int]);
      }
    } catch {
      case e: Throwable =>
        log.warn("unknow Error:" + e.getMessage, e)
    } finally {
      Thread.currentThread().setName(oldThreadName);
    }
  }
  //Scheduler.scheduleWithFixedDelay(new Runnable, initialDelay, delay, unit)
  def main(args: Array[String]): Unit = {
    URLHelper.init()
    //System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.bcapi.url");
    println(new URL("tcp://127.0.0.1:5100").getHost);
  }
}