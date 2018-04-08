package org.fc.brewchain.p22p.action

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.fc.brewchain.bcapi.crypto.EncHelper
import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import org.fc.brewchain.bcapi.exception.FBSException
import org.apache.commons.lang3.StringUtils
import java.util.HashSet
import onight.tfw.outils.serialize.UUIDGenerator
import scala.collection.JavaConversions._
import org.apache.commons.codec.binary.Base64
import org.fc.brewchain.p22p.pbgens.P22P.PSJoin
import org.fc.brewchain.p22p.pbgens.P22P.PRetJoin
import org.fc.brewchain.p22p.PSMPZP
import org.fc.brewchain.p22p.pbgens.P22P.PCommand
import java.net.URL
import org.fc.brewchain.p22p.pbgens.P22P.PMNodeInfo
import org.fc.brewchain.p22p.exception.NodeInfoDuplicated
import org.fc.brewchain.p22p.node.Networks
import org.fc.brewchain.p22p.node.PNode
import org.brewchain.bcapi.utils.PacketIMHelper._
import org.fc.brewchain.p22p.pbft.VoteWorker
import org.fc.brewchain.p22p.utils.LogHelper

@NActorProvider
@Slf4j
object PZPNodeJoin extends PSMPZP[PSJoin] {
  override def service = PZPNodeJoinService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPNodeJoinService extends LogHelper with PBUtils with LService[PSJoin] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSJoin, handler: CompleteHandler) = {
    log.debug("JoinService::" + pack.getFrom() + ",OP=" + pbo.getOp)
    var ret = PRetJoin.newBuilder();
    val network = networkByID(pbo.getNid)
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:" + pbo.getNid)
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(network)
        //       pbo.getMyInfo.getNodeName
        val from = pbo.getMyInfo;
        ret.setMyInfo(toPMNode(network.root))
        if (pbo.getOp == PSJoin.Operation.NODE_CONNECT) {
          val _urlcheck = new URL(from.getProtocol + "://" + from.getAddress + ":" + from.getPort)
          if ((from.getTryNodeIdx > 0 && from.getTryNodeIdx == network.root().node_idx) ||
            StringUtils.equals(from.getBcuid, network.root().bcuid)) {
            log.info("same NodeIdx :" + from.getNodeIdx + ",tryIdx=" + from.getTryNodeIdx + ",bcuid=" + from.getBcuid);
            throw new NodeInfoDuplicated("NodeIdx=" + from.getNodeIdx);
          } else if (network.node_bits.testBit(from.getTryNodeIdx)) {
            network.nodeByIdx(from.getTryNodeIdx) match {
              case Some(n) if n.bcuid.endsWith(from.getBcuid) =>
                log.debug("node backon line ")
                network.onlineMap.put(n.bcuid, n);
              case _ =>
                log.info("nodebits duplicated NodeIdx :" + from.getNodeIdx);
                throw new NodeInfoDuplicated("NodeIdx=" + from.getNodeIdx);
            }
          } else {
            //name, idx, protocol, address, port, startup_time, pub_key, counter,idx
            val n = fromPMNode(from);
            log.info("add Pending Node:bcuid=" + n.bcuid);
            network.onlineMap.put(n.bcuid, n);
            network.addPendingNode(n)
            try {
              VoteWorker.synchronized {
                log.debug("notify to wote");
                VoteWorker.notifyAll()
              }
            } finally {

            }
          }
        } else if (pbo.getOp == PSJoin.Operation.NODE_CONNECT) {
          //        NodeInstance.curnode.addPendingNode(new LinkNode(from.getProtocol, from.getNodeName, from.getAddress, // 
          //          from.getPort, from.getStartupTime, from.getPubKey, from.getTryNodeIdx, from.getNodeIdx))
        }

        //      ret.addNodes(toPMNode(NodeInstance.root));

        network.directNodes.map { _pn =>
          log.debug("direct.node==" + _pn.bcuid)
          ret.addNodes(toPMNode(_pn));
        }
        network.pendingNodes.map { _pn =>
          log.debug("pending.node==" + _pn.bcuid)
          ret.addNodes(toPMNode(_pn));
        }
      } catch {
        case fe: NodeInfoDuplicated => {
          ret.clear();
          ret.setMyInfo(toPMNode(network.root))
          ret.addNodes(toPMNode(network.root));
          ret.setRetCode(-1).setRetMessage(fe.getMessage)
        }
        case e: FBSException => {
          ret.clear()
          ret.setRetCode(-2).setRetMessage(e.getMessage)
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage(t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.JIN.name();
}
