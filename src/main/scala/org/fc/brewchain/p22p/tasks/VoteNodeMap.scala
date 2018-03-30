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
import org.fc.brewchain.p22p.pbft.StateStorage

//投票决定当前的节点
object VoteNodeMap extends SRunner {
  def getName() = "VoteNodeMap"
  def runOnce() = {
    try {
      log.debug("VoteNodeMap :Run----Try to Vote Node Maps");
      val oldThreadName = Thread.currentThread().getName + ""

      Thread.currentThread().setName("VoteNodeMap");
      log.info("CurrentPNodes:PendingSize=" + Networks.instance.pendingNodes.size + ",DirectNodeSize=" + Networks.instance.directNodes.size);
      val vbase = PVBase.newBuilder();
      val vbody = PBVoteNodeIdx.newBuilder();
      vbase.setState(PBFTStage.PRE_PREPARE)
      vbase.setMType(0)
      //    vbase.setMaxVid(value)
      vbase.setN(StateStorage.nextN())
      vbase.setV(StateStorage.curV());

      vbase.setFromBcuid(NodeInstance.root.bcuid);

      var bits = Networks.instance.node_bits;

      Networks.instance.pendingNodes.map(n =>
        if (bits.testBit(n.try_node_idx)) {
          log.debug("error in try_node_idx @n=" + n.name + ",try=" + n.try_node_idx + ",bits=" + bits);
        } else { //no pub keys
          bits = bits.setBit(n.try_node_idx);
          vbody.addNodes(toPMNode(n));
        })

      vbody.setNodeBitsEnc(BitMap.hexToMapping(bits))

      log.info("vote -- Nodes:" + vbody);
      vbase.setContents(toByteSting(vbody))
      //      vbase.addVoteContents(Any.pack(vbody.build()))
      if (Networks.instance.node_bits.bitCount <= 0) {
        log.debug("networks has not directnode!")
        //init. start to vote.
        val vbuild = vbase.build();
        Networks.instance.pendingNodes.map(n =>
          {
            MessageSender.postMessage("VOTPZP", vbuild, n)
          })
      }
      //    NodeInstance.forwardMessage("VOTPZP", vbody.build());
      //vbody.setNodeBitsEnc(bits.toString(16));

      Thread.sleep((Math.random() * 10000).asInstanceOf[Int]);
      Thread.currentThread().setName(oldThreadName);
      log.debug("Run-----[END]"); //
    } catch {
      case e: Throwable =>
        log.warn("unknow Error:", e)
    }
  }
  //Scheduler.scheduleWithFixedDelay(new Runnable, initialDelay, delay, unit)
  def main(args: Array[String]): Unit = {
    URLHelper.init()
    //System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.bcapi.url");
    println(new URL("tcp://127.0.0.1:5100").getHost);
  }
}