package org.fc.brewchain.p22p.pbft

import org.fc.brewchain.p22p.pbgens.P22P.PVBase
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteNodeIdx
import onight.oapi.scala.traits.OLog
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteViewChange
import org.fc.brewchain.p22p.core.Votes.VoteResult
import org.fc.brewchain.p22p.core.Votes.Undecisible
import org.fc.brewchain.p22p.core.Votes
import org.fc.brewchain.p22p.pbgens.P22P.PBFTStage
import org.brewchain.bcapi.gens.Oentity.OPair
import org.fc.brewchain.p22p.utils.Config
import org.fc.brewchain.p22p.Daos
import scala.collection.JavaConversions._
import org.fc.brewchain.p22p.pbgens.P22P.PVType

trait Votable extends OLog {
  def makeDecision(pbo: PVBase, reallist: List[OPair] = null): Option[Any]
  def finalConverge(pbo: PVBase): Unit
  def voteList(pbo: PVBase, reallist: List[OPair]): VoteResult = { // find max store num.
    val pboresult = if (pbo.getState == PBFTStage.PREPARE) {
      makeDecision(pbo, reallist);
    } else {
      0;
    }
    Votes.vote(reallist).PBFTVote({
      x =>
        val p = PVBase.newBuilder().mergeFrom(x.getValue.getExtdata).build();
        val dbresult = if (pbo.getState == PBFTStage.PREPARE) {
          makeDecision(p, reallist);
        } else {
          0;
        }
        //          log.debug("voteNodeStages::State=" + p.getState + ",Rejet=" + p.getRejectState + ",V=" + p.getV + ",N=" + p.getN + ",O=" + p.getOriginBcuid
        //            + ",F=" + p.getFromBcuid + ",KEY=" + new String(x.getKey.getData.toByteArray()) + ",OVS=" + x.getValue.getSecondKey)
        if (pbo.getCreateTime - p.getCreateTime > Config.TIMEOUT_STATE_VIEW_RESET) {
          log.debug("Force TIMEOUT node state to My State:" + p.getState + ",My=" + pbo.getState);
          Some(pbo.getState)
        } else if (pbo.getV == p.getV) {
          if (p.getRejectState == PBFTStage.REJECT || dbresult != pboresult) {
            Some(PBFTStage.REJECT)
          } else {
            Some(p.getState)
          }
        } else {
          None
        }
    }, pbo.getN)
  }
}

object DMVotingNodeBits extends Votable with OLog {
  def makeDecision(pbo: PVBase, reallist: List[OPair]): Option[String] = {
    val vb = PBVoteNodeIdx.newBuilder().mergeFrom(pbo.getContents);
    log.debug("makeDecision for DMVotingNodeBits:F=" + pbo.getFromBcuid + ",R=" + vb.getNodeBitsEnc);
    Some(vb.getNodeBitsEnc)
  }
  def finalConverge(pbo: PVBase): Unit = {
    val vb = PBVoteNodeIdx.newBuilder().mergeFrom(pbo.getContents);
    log.debug("FinalConverge! for DMVotingNodeBits:F=" + pbo.getFromBcuid + ",Result=" + vb.getNodeBitsEnc);
  }
}

object DMViewChange extends Votable with OLog {
  def makeDecision(pbo: PVBase, reallist: List[OPair]): Option[String] = {
    val vb = PBVoteViewChange.newBuilder().mergeFrom(pbo.getContents);
    val choise = vb.getStoreNum + "." + pbo.getV
    log.debug("makeDecision for DMViewChange:F=" + pbo.getFromBcuid + ",R=" + choise);
    val maxv = reallist.foldLeft(0)((A, kvs) => {
      val p = PVBase.newBuilder().mergeFrom(kvs.getValue.getExtdata);
      val vb = PBVoteViewChange.newBuilder().mergeFrom(p.getContents);
      Math.max(A, vb.getStoreNum)
    });
    //    val vb = PBVoteViewChange.newBuilder().mergeFrom(pbo.getContents)
    //      .setStoreNum(maxv);
    //    pbo
    Some(maxv + "." + choise)
  }
  override def voteList(pbo: PVBase, reallist: List[OPair]): VoteResult = { // find max store num.
    Votes.vote(reallist).PBFTVote({
      x =>
        val p = PVBase.newBuilder().mergeFrom(x.getValue.getExtdata).build();
        log.debug("voteNodeStages::State=" + p.getState + ",Rejet=" + p.getRejectState + ",V=" + p.getV + ",N=" + p.getN + ",O=" + p.getOriginBcuid
          + ",F=" + p.getFromBcuid + ",KEY=" + new String(x.getKey.getData.toByteArray()) + ",OVS=" + x.getValue.getSecondKey)
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
  }
  def finalConverge(pbo: PVBase): Unit = {
    val ovs = Daos.viewstateDB.listBySecondKey(StateStorage.STR_seq(pbo) + "." + pbo.getOriginBcuid + "." + pbo.getMessageUid + "." + pbo.getV);
    if (ovs.get != null && ovs.get.size() > 0) {
      val reallist = ovs.get.filter { ov => ov.getValue.getDecimals == pbo.getStateValue }.toList;
      log.debug("get list:allsize=" + ovs.get.size() + ",statesize=" + reallist.size + ",state=" + pbo.getState)
      //      outputList(ovs.get)
      val maxv = reallist.foldLeft(0)((A, kvs) => {
        val p = PVBase.newBuilder().mergeFrom(kvs.getValue.getExtdata);
        val vb = PBVoteViewChange.newBuilder().mergeFrom(p.getContents);
        Math.max(A, vb.getStoreNum)
      });
      val vb = PBVoteViewChange.newBuilder().mergeFrom(pbo.getContents).setStoreNum(maxv);

      StateStorage.updateTopViewState(pbo.toBuilder()
        .setViewCounter(0)
        .setMType(PVType.VOTE_IDX).setState(PBFTStage.REPLY)
        .setStoreNum(maxv).setContents(vb.build().toByteString()).build());
      //      dm.voteList(pbo, reallist)
      log.debug("FinalConverge! for DMViewChange:F:F=" + pbo.getFromBcuid + ",Result="+ maxv+"."+ vb.getStoreNum + "." + vb.getV);

    } else {
      Undecisible()
    }

  }

}