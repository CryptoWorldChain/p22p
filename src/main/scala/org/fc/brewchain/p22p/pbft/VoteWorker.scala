package org.fc.brewchain.p22p.pbft

import java.util.concurrent.atomic.AtomicInteger
import org.fc.brewchain.p22p.Daos
import org.apache.commons.lang3.StringUtils
import onight.oapi.scala.traits.OLog
import org.fc.brewchain.p22p.pbgens.P22P.PVBase
import org.fc.brewchain.p22p.pbgens.P22P.PBFTStage
import scala.concurrent.impl.Future
import com.google.protobuf.ByteString
import onight.tfw.otransio.api.PacketHelper
import org.fc.brewchain.p22p.node.NodeInstance
import org.fc.brewchain.p22p.pbgens.P22P.PVType
import org.fc.brewchain.p22p.pbgens.P22P.PVBaseOrBuilder
import org.fc.brewchain.bcapi.JodaTimeHelper
import org.brewchain.bcapi.gens.Oentity.OValue
import java.util.ArrayList
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import org.fc.brewchain.p22p.core.Votes
import org.fc.brewchain.p22p.core.Votes.VoteResult
import org.fc.brewchain.p22p.core.Votes.NotConverge
import org.brewchain.bcapi.gens.Oentity.OValueOrBuilder
import org.fc.brewchain.p22p.core.Votes.Converge
import org.fc.brewchain.p22p.core.Votes.Undecisible
import onight.tfw.mservice.NodeHelper
import org.fc.brewchain.p22p.utils.Config
import org.apache.commons.codec.binary.Base64
import org.fc.brewchain.p22p.utils.LogHelper
import org.fc.brewchain.p22p.tasks.SRunner
import org.fc.brewchain.p22p.core.MessageSender
import org.fc.brewchain.p22p.node.Networks

object VoteWorker extends SRunner with LogHelper {

  def getName() = "VoteWorker"

  def wallMessage(pbo: PVBase) = {
    Networks.instance.pendingNodes.map { node =>
      MessageSender.postMessage("VOTPZP", pbo, node.bcuid)
    }
  }
  def makeVote(pbo: PVBase, ov: OValue.Builder, newstate: PBFTStage) = {
    MDCSetMessageID(pbo.getMessageUid)

    log.debug("makeVote:State=" + pbo.getState + ",trystate=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);
    val reply = pbo.toBuilder().setState(newstate)
      .setFromBcuid(NodeInstance.root().bcuid)
      .setOldState(pbo.getState);

    newstate match {
      case PBFTStage.PREPARE =>
        log.debug("Vote::Move TO Next=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);
        wallMessage(reply.build());
      //        PBFTStage.PREPARE
      case PBFTStage.REJECT =>
        reply.setState(pbo.getState).setRejectState(PBFTStage.REJECT)
        //        log.info("MergeSuccess.Local!:V=" + pbo.getV + ",N=" + pbo.getN + ",org=" + pbo.getOriginBcuid)
        MessageSender.replyPostMessage("VOTPZP", pbo.getFromBcuid, reply.build());
      //      case PBFTStage.REPLY =>
      //        StateStorage.saveStageV(pbo, ov.build());
      //        log.info("MergeSuccess.Local!:V=" + pbo.getV + ",N=" + pbo.getN + ",org=" + pbo.getOriginBcuid)
      case _ =>
        StateStorage.makeVote(pbo, ov, newstate) match {
          case PBFTStage.COMMIT => //|| s == PBFTStage.REPLY =>
            log.debug("Vote::Move TO Next=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);
            wallMessage(reply.build())
          case PBFTStage.REJECT =>
            log.debug("Vote::Reject =" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);
            reply.setState(pbo.getState).setRejectState(PBFTStage.REJECT)
            MessageSender.replyPostMessage("VOTPZP", pbo.getFromBcuid, reply.build());
          case PBFTStage.REPLY =>
            StateStorage.saveStageV(pbo, ov.build());
            log.info("MergeSuccess.Local!:V=" + pbo.getV + ",N=" + pbo.getN + ",org=" + pbo.getOriginBcuid)
          case PBFTStage.DUPLICATE =>
            log.info("Duplicated Vote Message!:V=" + pbo.getV + ",N=" + pbo.getN +",State="+pbo.getState+ ",org=" + pbo.getOriginBcuid)
          case s @ _ =>
            log.debug("Noop for state:" + newstate + ",voteresult=" + s)

        }
    }
  }
  def runOnce() = {
    try {
      MDCSetBCUID();
      //      var (pbo, ov, newstate) = VoteQueue.pollQ();
      var hasWork = true;
      log.debug("Get Q size=" + VoteQueue.inQ.size())
      while (hasWork) {
        val q = VoteQueue.pollQ();
        if (q == null) {
          hasWork = false;
        } else {
          makeVote(q._1, q._2, q._3)
        }
      }
    } catch {
      case e: Throwable =>
        log.warn("unknow Error:" + e.getMessage, e)
    } finally {
      MDCRemoveMessageID()

    }
  }
}