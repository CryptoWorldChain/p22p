package org.fc.brewchain.p22p.pbft

import java.util.concurrent.atomic.AtomicInteger
import org.fc.brewchain.p22p.Daos
import org.apache.commons.lang3.StringUtils
import onight.oapi.scala.traits.OLog
import org.fc.brewchain.p22p.pbgens.P22P.PVBase
import org.fc.brewchain.p22p.pbgens.P22P.PBFTStage
import org.brewchain.bcapi.gens.Odb.OKey
import scala.concurrent.impl.Future
import org.brewchain.bcapi.gens.Odb.OValue
import com.google.protobuf.ByteString
import onight.tfw.otransio.api.PacketHelper
import org.fc.brewchain.p22p.node.NodeInstance
import org.fc.brewchain.p22p.pbgens.P22P.PVType
import org.fc.brewchain.p22p.pbgens.P22P.PVBaseOrBuilder
import org.brewchain.bcapi.gens.Odb.OValueOrBuilder
import org.fc.brewchain.bcapi.JodaTimeHelper

object StateStorage extends OLog {
  def MAX_VIEW_TIMEOUT_MS = 120 * 1000;
  def STR_seq(pbo: PVBaseOrBuilder): String = STR_seq(pbo.getMTypeValue)
  def STR_seq(uid: Int): String = "v_seq_" + uid

  def nextV(pbo: PVBase.Builder): Int = {
    val (retv, newstate) = Daos.viewstateDB.get(STR_seq(pbo)).get match {
      case ov if ov != null =>
        PVBase.newBuilder().mergeFrom(ov.getExtdata) match {
          case dbpbo if dbpbo.getState != PBFTStage.COMMIT && System.currentTimeMillis() - dbpbo.getCreateTime > MAX_VIEW_TIMEOUT_MS =>
            log.debug("vote:" + dbpbo.getState);
            (dbpbo.getV, PBFTStage.PRE_PREPARE)
          case dbpbo if dbpbo.getState == PBFTStage.COMMIT || dbpbo.getState == PBFTStage.REPLY =>
            (dbpbo.getV + 1, PBFTStage.PRE_PREPARE)
          case dbpbo =>
            log.debug("cannot vote:" + dbpbo.getState + ",LVT=" + JodaTimeHelper.format(dbpbo.getCreateTime) + ",bcuid=" + dbpbo.getFromBcuid);
            (-1, PBFTStage.REJECT);
        }
      case _ =>
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

  def getLocalViewState(pbo: PVBase): Option[OValue.Builder] = {
    Daos.viewstateDB.get(STR_seq(pbo)).get match {
      case ov if ov != null && StringUtils.equals(pbo.getFromBcuid, NodeInstance.root().bcuid) =>
        Some(ov.toBuilder()) // from locals
      case ov if ov != null =>
        PVBase.newBuilder().mergeFrom(ov.getExtdata) match {
          case dbpbo if StringUtils.equals(dbpbo.getFromBcuid, pbo.getFromBcuid) && pbo.getN == dbpbo.getN
            && dbpbo.getStateValue <= dbpbo.getStateValue =>
            Some(ov.toBuilder());
          case dbpbo if System.currentTimeMillis() - dbpbo.getCreateTime > MAX_VIEW_TIMEOUT_MS
            && pbo.getCreateTime - dbpbo.getCreateTime > MAX_VIEW_TIMEOUT_MS =>
            Some(ov.toBuilder());
          case dbpbo if pbo.getV > dbpbo.getV && dbpbo.getState == PBFTStage.COMMIT =>
            Some(ov.toBuilder());
          case _ =>
            None;
        }
      case _ =>
        val ov = OValue.newBuilder().setCount(pbo.getN) //
          .setExtdata(ByteString.copyFrom(pbo.toByteArray()))
        Some(ov)
    }
  }
  def updateLocalViewState(pbo: PVBase, ov: OValue.Builder, newstate: PBFTStage): PBFTStage = {
    ov.setExtdata(
      ByteString.copyFrom(pbo.toBuilder()
        .setState(newstate)
        .build().toByteArray()))

    Daos.viewstateDB.put(STR_seq(pbo), ov.build());
    Daos.viewstateDB.sync();
    newstate
  }

  def vote(pbo: PVBase): PBFTStage = {
    //    val ov = ;
    getLocalViewState(pbo) match {
      case Some(ov) =>
        pbo.getState match {
          case PBFTStage.PRE_PREPARE =>
            //            Daos.viewstateDB.put(STR_seq(pbo), ov.build());
            updateLocalViewState(pbo, ov, PBFTStage.PREPARE)
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
  val VIEW_ID_PROP = "org.bc.pbft.view.state"

}