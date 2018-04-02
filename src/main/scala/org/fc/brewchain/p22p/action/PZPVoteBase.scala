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
import org.fc.brewchain.p22p.pbgens.P22P.PVBase
import onight.tfw.mservice.NodeHelper
import com.google.protobuf.Any
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteNodeIdx
import org.fc.brewchain.p22p.pbgens.P22P.PVType
import org.fc.brewchain.p22p.Daos
import org.fc.brewchain.p22p.pbft.StateStorage
import org.fc.brewchain.p22p.pbgens.P22P.PBFTStage
import org.fc.brewchain.p22p.core.MessageSender
import org.brewchain.bcapi.utils.PacketIMHelper._
import org.slf4j.MDC
import org.fc.brewchain.p22p.utils.LogHelper
import org.fc.brewchain.p22p.utils.LogHelper
import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.node.Networks
import org.fc.brewchain.bcapi.BCPacket
import org.fc.brewchain.p22p.pbft.VoteQueue

@NActorProvider
@Slf4j
object PZPVoteBase extends PSMPZP[PVBase] {
  override def service = PZPVoteBaseService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PZPVoteBaseService extends OLog with PBUtils with LService[PVBase] with PMNodeHelper with LogHelper {
  override def onPBPacket(pack: FramePacket, pbo: PVBase, handler: CompleteHandler) = {
    MDCSetMessageID(pbo.getMessageUid)
    if (NodeInstance.root() != null) {
      MDCSetBCUID()
    }

    log.info("VoteBase:MType=" + pbo.getMType + ":State=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",O=" + pbo.getOriginBcuid + ",F=" + pbo.getFromBcuid
      + ",Rejct=" + pbo.getRejectState)

    var ret = PRetJoin.newBuilder();
    try {
      pbo.getMType match {
        case PVType.VOTE_IDX =>
          VoteQueue.appendInQ(pbo)
//          
//          val nextstate = StateStorage.vote(pbo);
//          if (pbo.getRejectState != PBFTStage.REJECT) {
//            val reply = pbo.toBuilder().setState(nextstate).setFromBcuid(NodeInstance.root().bcuid)
//              .setOldState(pbo.getState);
//            nextstate match {
//              case PBFTStage.REJECT =>
//                reply.setState(pbo.getState).setRejectState(PBFTStage.REJECT)
//                MessageSender.replyPostMessage(pack.getGlobalCMD,pbo.getFromBcuid, reply.build());
//              case PBFTStage.NOOP =>
//              case PBFTStage.REPLY =>
//                log.info("MergeSuccess!!:V=" + pbo.getV + ",N=" + pbo.getN + ",org=" + pbo.getOriginBcuid)
//              //do nothing.
//              case _ =>
//                Networks.instance.pendingNodes.map { node =>
//                  MessageSender.postMessage(pack.getModuleAndCMD, reply.build(), node.bcuid)
//                }
//            }
//          } else {
//            log.debug("Reject status will not wall message again");
//          }
        case _ =>
          log.debug("unknow vote message:type=" + pbo.getMType)
      }

      //      }
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
      try {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      } finally {
        MDCRemoveMessageID
      }

    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.VOT.name();
}
