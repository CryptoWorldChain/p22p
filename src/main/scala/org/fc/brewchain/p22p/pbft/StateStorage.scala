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
import org.brewchain.bcapi.gens.Oentity.OPair
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

object StateStorage extends OLog {
  def STR_seq(pbo: PVBaseOrBuilder): String = STR_seq(pbo.getMTypeValue)
  def STR_seq(uid: Int): String = "v_seq_" + uid

  def nextV(pbo: PVBase.Builder): Int = {
    this.synchronized {
      val (retv, newstate) = Daos.viewstateDB.get(STR_seq(pbo)).get match {
        case ov if ov != null =>
          PVBase.newBuilder().mergeFrom(ov.getExtdata) match {
            case dbpbo if dbpbo.getState == PBFTStage.REPLY =>
              if (System.currentTimeMillis() - dbpbo.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE) {
                log.debug("cannot start next vote: time less than past" + dbpbo.getState + ",V=" + dbpbo.getV + ",DIS=" + JodaTimeHelper.secondFromNow(dbpbo.getLastUpdateTime));
                (-1, PBFTStage.REJECT);
              } else {
                log.debug("getting next vote:" + dbpbo.getState + ",V=" + dbpbo.getV);
                (dbpbo.getV + 1, PBFTStage.PRE_PREPARE)
              }
            case dbpbo if System.currentTimeMillis() - dbpbo.getLastUpdateTime < Config.MIN_EPOCH_EACH_VOTE && dbpbo.getState == PBFTStage.INIT || System.currentTimeMillis() - dbpbo.getCreateTime > Config.TIMEOUT_STATE_VIEW =>
              log.debug("recover from vote:" + dbpbo.getState + ",lastCreateTime:" + JodaTimeHelper.format(dbpbo.getCreateTime));
              (dbpbo.getV, PBFTStage.PRE_PREPARE)
            case dbpbo =>
              log.debug("cannot start vote:" + dbpbo.getState + ",past=" + JodaTimeHelper.secondFromNow(dbpbo.getCreateTime) + ",O=" + dbpbo.getOriginBcuid);
              (-1, PBFTStage.REJECT);
          }
        case _ =>
          log.debug("New State ,db is empty");
          (1, PBFTStage.PRE_PREPARE);
      }
      if (Config.VOTE_DEBUG) return -1;
      if (retv > 0) {
        Daos.viewstateDB.put(STR_seq(pbo),
          OValue.newBuilder().setCount(retv) //
            .setExtdata(ByteString.copyFrom(pbo.setV(retv)
              .setCreateTime(System.currentTimeMillis())
              .setLastUpdateTime(System.currentTimeMillis())
              .setFromBcuid(NodeInstance.root().bcuid)
              .setState(newstate)
              .build().toByteArray()))
            .build());
      }
      retv
    }
  }

  def mergeViewState(pbo: PVBase): Option[OValue.Builder] = {
    Daos.viewstateDB.get(STR_seq(pbo)).get match {
      case ov if ov != null && StringUtils.equals(pbo.getOriginBcuid, NodeInstance.root().bcuid) =>
        Some(ov.toBuilder()) // from locals
      case ov if ov != null =>
        PVBase.newBuilder().mergeFrom(ov.getExtdata) match {
          case dbpbo if StringUtils.equals(dbpbo.getOriginBcuid, pbo.getOriginBcuid) && pbo.getV == dbpbo.getV
            && dbpbo.getStateValue <= pbo.getStateValue =>
            Some(ov.toBuilder());
          case dbpbo if StringUtils.equals(dbpbo.getOriginBcuid, pbo.getOriginBcuid) && pbo.getV == dbpbo.getV
            && dbpbo.getStateValue > pbo.getStateValue =>
            log.debug("state low dbV=" + dbpbo.getStateValue + ",pbV=" + pbo.getStateValue + ",V=" + pbo.getV + ",f=" + pbo.getFromBcuid)
            Some(null)
          case dbpbo if System.currentTimeMillis() - dbpbo.getCreateTime > Config.TIMEOUT_STATE_VIEW
            && pbo.getV > dbpbo.getV =>
            Some(ov.toBuilder());
          case dbpbo if pbo.getV >= dbpbo.getV && (
            dbpbo.getState == PBFTStage.INIT || dbpbo.getState == PBFTStage.REPLY) =>
            Some(ov.toBuilder());
          case dbpbo @ _ =>
            pbo.getState match {
              case PBFTStage.COMMIT => //already know by other.
                log.debug("other nodes commited!")
                updateNodeStage(pbo, PBFTStage.COMMIT);
                voteNodeStages(pbo) match {
                  case n: Converge if n.decision == pbo.getState =>
                    log.debug("OtherVoteCommit::MergeOK,PS=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid + ",from=" + pbo.getFromBcuid);
                    Daos.viewstateDB.put(STR_seq(pbo),
                      OValue.newBuilder().setCount(pbo.getN) //
                        .setExtdata(ByteString.copyFrom(pbo.toBuilder()
                          .setFromBcuid(NodeInstance.root().bcuid)
                          .setState(PBFTStage.INIT)
                          .build().toByteArray()))
                        .build())
                    None;
                  case _ =>
                    log.debug("OtherVoteCommit::NotMerge,PS=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid + ",from=" + pbo.getFromBcuid);
                    None;
                }
              case _ =>
                log.debug("Cannot MergeView For local state Not EQUAL:"
                  + "db.[O=" + dbpbo.getOriginBcuid + ",F=" + dbpbo.getFromBcuid + ",V=" + dbpbo.getV + ",S=" + dbpbo.getStateValue
                  + "],p[O=" + pbo.getOriginBcuid + ",F=" + pbo.getFromBcuid + ",V=" + pbo.getV + ",S=" + pbo.getStateValue + "]");
                None;
            }

        }
      case _ =>
        val ov = OValue.newBuilder().setCount(pbo.getN) //
          .setExtdata(ByteString.copyFrom(pbo.toByteArray()))
        Some(ov)
    }
  }

  def saveStageV(pbo: PVBase, ov: OValue) {
    val key = STR_seq(pbo) + ".F." + pbo.getV;
    //    val dbov = Daos.viewstateDB.get(key).get
    log.debug("saveStage:V=" + pbo.getV + ",O=" + pbo.getOriginBcuid)
    //    if (dbov != null) {
    //      val pb = PVBase.newBuilder().mergeFrom(ov.getExtdata);
    //      pb.setContents(ByteString.copyFrom(Base64.encodeBase64(pb.getContents.toByteArray()))).setLastUpdateTime(System.currentTimeMillis())
    //      log.warn("Already Save Stage!" + dbov + ",pb=" + pb)
    //    }
    Daos.viewstateDB.put(STR_seq(pbo) + ".F." + pbo.getV, ov);
  }

  def updateLocalViewState(pbo: PVBase, ov: OValue.Builder, newstate: PBFTStage): PBFTStage = {
    updateNodeStage(pbo, pbo.getState)
    makeVote(pbo, ov, newstate)
  }
  def makeVote(pbo: PVBase, ov: OValue.Builder, newstate: PBFTStage): PBFTStage = {
    val ret = voteNodeStages(pbo) match {
      case n: Converge if n.decision == pbo.getState =>
        ov.setExtdata(
          ByteString.copyFrom(pbo.toBuilder()
            .setState(newstate)
            .setLastUpdateTime(System.currentTimeMillis())
            .build().toByteArray()))
        log.debug("Vote::MergeOK,PS=" + pbo.getState + ",New=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid + ",from=" + pbo.getFromBcuid);
        Daos.viewstateDB.put(STR_seq(pbo), ov.clone().clearSecondKey().build());
        newstate
      case un: Undecisible =>
        log.debug("Vote::Undecisible:State=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);
        PBFTStage.NOOP
      case no: NotConverge =>
        log.debug("Vote::Not Converge:State=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);
        if (StringUtils.equals(pbo.getOriginBcuid, NodeInstance.root().bcuid)) {
          Daos.viewstateDB.put(STR_seq(pbo),
            OValue.newBuilder().setCount(pbo.getN) //
              .setExtdata(ByteString.copyFrom(pbo.toBuilder()
                .setFromBcuid(NodeInstance.root().bcuid)
                .setState(PBFTStage.INIT)
                .setLastUpdateTime(System.currentTimeMillis())
                .build().toByteArray()))
              .build())
        }
        PBFTStage.NOOP
      case n: Converge if n.decision == PBFTStage.REJECT =>
        log.warn("getRject ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState);
        if (StringUtils.equals(pbo.getOriginBcuid, NodeInstance.root().bcuid)) {
          Daos.viewstateDB.put(STR_seq(pbo),
            OValue.newBuilder().setCount(pbo.getN) //
              .setExtdata(ByteString.copyFrom(pbo.toBuilder()
                .setFromBcuid(NodeInstance.root().bcuid)
                .setState(PBFTStage.INIT)
                .setLastUpdateTime(System.currentTimeMillis())
                .build().toByteArray()))
              .build())
        }
        PBFTStage.REJECT
      case n: Converge =>
        log.warn("unknow ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState);
        PBFTStage.NOOP
      case _ =>
        PBFTStage.NOOP
    }
    ret
  }
  def updateNodeStage(pbo: PVBase, state: PBFTStage): PBFTStage = {
    val strkey = STR_seq(pbo);
    val newpbo = if (state != pbo.getState) pbo.toBuilder().setState(state).setLastUpdateTime(System.currentTimeMillis()).build()
    else
      pbo;
    val ov = OValue.newBuilder().setCount(pbo.getN) //
    ov.setExtdata(ByteString.copyFrom(newpbo.toByteArray()))
      .setSecondKey(strkey + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV)
      .setDecimals(newpbo.getStateValue);

    Daos.viewstateDB.put(strkey + "." + pbo.getFromBcuid + "." + pbo.getMessageUid + "." + pbo.getV + "." + newpbo.getStateValue, ov.build());
    state
  }
  def outputList(ovs: List[OPair]):Unit = {
    ovs.map { x =>
      val p = PVBase.newBuilder().mergeFrom(x.getValue.getExtdata);
      log.debug("-------::DBList:State=" + p.getState + ",V=" + p.getV + ",N=" + p.getN + ",O=" + p.getOriginBcuid
        + ",F=" + p.getFromBcuid + ",REJRECT=" + p.getRejectState + ",KEY=" + new String(x.getKey.getData.toByteArray()))
    }
  }
  def voteNodeStages(pbo: PVBase): VoteResult = {
    val strkey = STR_seq(pbo);
    val ovs = Daos.viewstateDB.listBySecondKey(strkey + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV);
    if (ovs.get != null && ovs.get.size() > 0) {
      val reallist = ovs.get.filter { ov => ov.getValue.getDecimals == pbo.getStateValue }.toList;
      log.debug("get list:allsize=" + ovs.get.size() + ",statesize=" + reallist.size + ",state=" + pbo.getState)
//      outputList(ovs.get)
      //      ovs.get.map { x =>
      //        val p = PVBase.newBuilder().mergeFrom(x.getValue.getExtdata);
      //        log.debug("-------::DBList:State=" + p.getState + ",V=" + p.getV + ",N=" + p.getN + ",O=" + p.getOriginBcuid
      //          + ",F=" + p.getFromBcuid + ",REJRECT=" + p.getRejectState + ",KEY=" + new String(x.getKey.getData.toByteArray()))
      //      }
      //      outpu
      //            val l = List("aa", "bb", "cc",  "aa", "aa")
      //            Votes.vote(l).RCPTVote { x => ??? }
      //          println("pbft.vote=" + l.RCPTVote().decision);
      Votes.vote(reallist).PBFTVote({
        x =>
          val p = PVBase.newBuilder().mergeFrom(x.getValue.getExtdata);
//          log.debug("voteNodeStages::State=" + p.getState + ",Rejet=" + p.getRejectState + ",V=" + p.getV + ",N=" + p.getN + ",O=" + p.getOriginBcuid
//            + ",F=" + p.getFromBcuid + ",KEY=" + new String(x.getKey.getData.toByteArray()) + ",OVS=" + x.getValue.getSecondKey)
          if (pbo.getCreateTime - p.getCreateTime > Config.TIMEOUT_STATE_VIEW_RESET) {
            log.debug("Force TIMEOUT node state to My State:" + p.getState + ",My=" + pbo.getState);
            Some(pbo.getState)
          } else if (pbo.getV == p.getV) {
            if (p.getRejectState == PBFTStage.REJECT) {
              Some(PBFTStage.REJECT)
            } else {
              Some(p.getState)
            }
          } else {
            None
          }
      }, pbo.getN)
    } else {
      Undecisible()
    }
  }

  def vote(pbo: PVBase): PBFTStage = {
    this.synchronized {
      mergeViewState(pbo) match {
        case Some(ov) if ov == null =>
          PBFTStage.NOOP
        case Some(ov) if ov != null =>
          pbo.getState match {
            case PBFTStage.PRE_PREPARE =>
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              //updateLocalViewState(pbo, ov, PBFTStage.PREPARE)
              updateNodeStage(pbo, PBFTStage.PREPARE)
              if (pbo.getRejectState == PBFTStage.REJECT) {
                PBFTStage.NOOP
              } else {
                PBFTStage.PREPARE
              }
            case PBFTStage.PREPARE =>
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              updateLocalViewState(pbo, ov, PBFTStage.COMMIT)
            case PBFTStage.COMMIT =>
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              updateLocalViewState(pbo, ov, PBFTStage.REPLY)
            case PBFTStage.REPLY =>
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              saveStageV(pbo, ov.build());
              PBFTStage.NOOP
            //            case PBFTStage.REJECT =>
            //              log.debug("get Reject from=" + pbo.getFromBcuid + ",V=" + pbo.getV + ",O=" + pbo.getOriginBcuid + ",OLDState=" + pbo.getOldState);
            //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
            //              updateLocalViewState(pbo, ov, pbo.getOldState)
            //              PBFTStage.NOOP
            case _ =>
              PBFTStage.NOOP
          }

        case None =>
          PBFTStage.REJECT
      }
    }

  }
  val VIEW_ID_PROP = "org.bc.pbft.view.state"

}