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
import org.fc.brewchain.p22p.node.NodeInstance
import java.net.URL
import org.fc.brewchain.p22p.pbgens.P22P.PMNodeInfo
import org.fc.brewchain.p22p.exception.NodeInfoDuplicated
import org.fc.brewchain.p22p.pbgens.P22P.PSNodeInfo
import org.fc.brewchain.p22p.pbgens.P22P.PRetNodeInfo
import org.fc.brewchain.p22p.node.Networks

import org.brewchain.bcapi.utils.PacketIMHelper._
import org.fc.brewchain.p22p.utils.LogHelper
import org.fc.brewchain.bcapi.crypto.BitMap

@NActorProvider
@Slf4j
object PZPHeatBeat extends PSMPZP[PSNodeInfo] {
  override def service = PZPHeatBeatService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPHeatBeatService extends OLog with PBUtils with LService[PSNodeInfo] with PMNodeHelper with LogHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSNodeInfo, handler: CompleteHandler) = {
//    log.debug("onPBPacket::" + pbo)
    log.debug("get HBT from:"+pack.getFrom())
    var ret = PRetNodeInfo.newBuilder();
    try {
      //       pbo.getMyInfo.getNodeName
      ret.setCurrent(toPMNode(NodeInstance.root))
      val pending = Networks.instance.pendingNodes;
      val directNodes = Networks.instance.directNodes;
      log.debug("pending=" + Networks.instance.pendingNodes.size + "::" + Networks.instance.pendingNodes)
      //      ret.addNodes(toPMNode(NodeInstance.curnode));
      pending.map { _pn =>
        log.debug("pending==" + _pn)
        ret.addPnodes(toPMNode(_pn));
      }
      directNodes.map { _pn =>
        log.debug("directnodes==" + _pn)
        ret.addDnodes(toPMNode(_pn));
      }
      ret.setBitEncs(BitMap.hexToMapping(Networks.instance.node_bits));

    } catch {
      case fe: NodeInfoDuplicated => {
        ret.clear();
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
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.HBT.name();
}
