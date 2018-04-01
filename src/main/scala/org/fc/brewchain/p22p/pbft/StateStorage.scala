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

object StateStorage extends OLog {
  def MAX_VIEW_TIMEOUT_MS = 120 * 1000;
  def MAX_RESET_VIEW_TIMEOUT_MS = 360 * 1000;
  def STR_seq(pbo: PVBaseOrBuilder): String = STR_seq(pbo.getMTypeValue)
  def STR_seq(uid: Int): String = "v_seq_" + uid

  def nextV(pbo: PVBase.Builder): Int = {
    val (retv, newstate) = Daos.viewstateDB.get(STR_seq(pbo)).get match {
      case ov if ov != null =>
        PVBase.newBuilder().mergeFrom(ov.getExtdata) match {
          case dbpbo if dbpbo.getState == PBFTStage.REPLY =>
            log.debug("getting next vote:" + dbpbo.getState + ",V=" + dbpbo.getV);
            (dbpbo.getV + 1, PBFTStage.PRE_PREPARE)
          case dbpbo if dbpbo.getState == PBFTStage.INIT || System.currentTimeMillis() - dbpbo.getCreateTime > MAX_VIEW_TIMEOUT_MS =>
            log.debug("recover from vote:" + dbpbo.getState + ",lastCreateTime:" + JodaTimeHelper.format(dbpbo.getCreateTime));
            (dbpbo.getV, PBFTStage.PRE_PREPARE)
          case dbpbo =>
            log.debug("cannot start vote:" + dbpbo.getState + ",LVT=" + JodaTimeHelper.format(dbpbo.getCreateTime) + ",bcuid=" + dbpbo.getFromBcuid);
            (-1, PBFTStage.REJECT);
        }
      case _ =>
        log.debug("New State ,db is empty");
        (1, PBFTStage.PRE_PREPARE);
    }
    if (retv > 0) {
      Daos.viewstateDB.put(STR_seq(pbo),
        OValue.newBuilder().setCount(retv) //
          .setExtdata(ByteString.copyFrom(pbo.setV(retv)
            .setCreateTime(System.currentTimeMillis())
            .setFromBcuid(NodeInstance.root().bcuid)
            .setState(newstate)
            .build().toByteArray()))
          .build());
    }
    retv
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
          case dbpbo if System.currentTimeMillis() - dbpbo.getCreateTime > MAX_VIEW_TIMEOUT_MS
            && pbo.getV > dbpbo.getV =>
            Some(ov.toBuilder());
          case dbpbo if pbo.getV > dbpbo.getV && (
            dbpbo.getState == PBFTStage.INIT || dbpbo.getState == PBFTStage.REPLY) =>
            Some(ov.toBuilder());
          case dbpbo @ _ =>
            log.debug("Cannot MergeView For local state Not EQUAL:DBState=" + dbpbo.getState + ",V=" + pbo.getV
              + ",DBV=" + dbpbo.getV
              + ",db.[O=" + dbpbo.getOriginBcuid + ",F=" + dbpbo.getFromBcuid
              + "],p[O=" + pbo.getOriginBcuid + ",F=" + pbo.getFromBcuid + "]");
            None;
        }
      case _ =>
        val ov = OValue.newBuilder().setCount(pbo.getN) //
          .setExtdata(ByteString.copyFrom(pbo.toByteArray()))
        Some(ov)
    }
  }
  def updateLocalViewState(pbo: PVBase, ov: OValue.Builder, newstate: PBFTStage): PBFTStage = {

    updateNodeStage(pbo, pbo.getState)

    voteNodeStages(pbo) match {
      case n: Converge if n.decision == pbo.getState =>
        ov.setExtdata(
          ByteString.copyFrom(pbo.toBuilder()
            .setState(newstate)
            .build().toByteArray()))
        log.debug("Vote::MergeOK,PS=" + pbo.getState + ",New=" + newstate + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);

        Daos.viewstateDB.put(STR_seq(pbo), ov.clone().clearSecondKey().build());
        newstate
      case un: Undecisible =>
        log.debug("Vote::Undecisible:State=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);
        PBFTStage.NOOP
      case no: NotConverge =>
        log.debug("Vote::Not Converge:State=" + pbo.getState + ",V=" + pbo.getV + ",N=" + pbo.getN + ",org_bcuid=" + pbo.getOriginBcuid);
        //        PBFTStage.NOOP
        Daos.viewstateDB.put(STR_seq(pbo),
          OValue.newBuilder().setCount(pbo.getN) //
            .setExtdata(ByteString.copyFrom(pbo.toBuilder()
              .setFromBcuid(NodeInstance.root().bcuid)
              .setState(PBFTStage.INIT)
              .build().toByteArray()))
            .build())
        PBFTStage.NOOP
      case n: Converge =>
        log.warn("unknow ConvergeState:" + n.decision + ",NewState=" + newstate + ",pbostate=" + pbo.getState);
        PBFTStage.NOOP
    }

  }
  def updateNodeStage(pbo: PVBase, state: PBFTStage): PBFTStage = {
    val strkey = STR_seq(pbo);
    val newpbo = if (state != pbo.getState) pbo.toBuilder().setState(state).build()
    else
      pbo;
    val ov = OValue.newBuilder().setCount(pbo.getN) //
    if (state == PBFTStage.PREPARE) {
      //      ov.setExtdata(ByteString.copyFrom(newpbo.toByteArray()))
      //      Daos.viewstateDB.put(STR_seq(pbo), ov.build());
    }
    ov.setExtdata(ByteString.copyFrom(newpbo.toByteArray()))
      .setSecondKey(strkey + "." + pbo.getOriginBcuid + "." + pbo.getV);
    Daos.viewstateDB.put(strkey + "." + pbo.getFromBcuid, ov.build());
    state
  }

  def voteNodeStages(pbo: PVBase): VoteResult = {
    val strkey = STR_seq(pbo);
    val ovs = Daos.viewstateDB.listBySecondKey(strkey + "." + pbo.getOriginBcuid + "." + pbo.getV);
    if (ovs.get != null && ovs.get.size() > 0) {
      log.debug("get list:size=" + ovs.get.size())
      //            val l = List("aa", "bb", "cc",  "aa", "aa")
      //            Votes.vote(l).RCPTVote { x => ??? }
      //          println("pbft.vote=" + l.RCPTVote().decision);
      Votes.vote(ovs.get.toList).PBFTVote({
        x =>
          val p = PVBase.newBuilder().mergeFrom(x.getValue.getExtdata);
          log.debug("voteNodeStages::State=" + p.getState + ",V=" + p.getV + ",N=" + p.getN + ",O=" + p.getOriginBcuid
            + ",F=" + p.getFromBcuid + ",KEY=" + new String(x.getKey.getData.toByteArray()) + ",OVS=" + x.getValue.getSecondKey)
          if (pbo.getCreateTime - p.getCreateTime > MAX_RESET_VIEW_TIMEOUT_MS) {
            log.debug("Force TIMEOUT node state to My State:" + p.getState + ",My=" + pbo.getState);
            Some(pbo.getState)
          } else if (pbo.getV == p.getV) {
            Some(p.getState)
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
      //    val ov = ;

      mergeViewState(pbo) match {
        case Some(ov) =>
          pbo.getState match {
            case PBFTStage.PRE_PREPARE =>
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              //updateLocalViewState(pbo, ov, PBFTStage.PREPARE)

              updateNodeStage(pbo, PBFTStage.PREPARE)
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
              PBFTStage.NOOP
            case PBFTStage.REJECT =>
              //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
              PBFTStage.NOOP
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