package org.fc.brewchain.p22p.pbft

import org.fc.brewchain.p22p.utils.LogHelper
import java.util.concurrent.ConcurrentLinkedQueue
import org.fc.brewchain.p22p.pbgens.P22P.PVBase
import org.brewchain.bcapi.gens.Oentity.OValue
import org.fc.brewchain.p22p.pbgens.P22P.PBFTStage
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

object VoteQueue extends LogHelper {

  val inQ = new LinkedBlockingQueue[(PVBase, OValue.Builder, PBFTStage)]();
  val outQ = new ConcurrentLinkedQueue[PVBase]();

  def appendInQ(pbo: PVBase) = {
    StateStorage.mergeViewState(pbo) match {
      case Some(ov) if ov == null =>
        PBFTStage.NOOP
      case Some(ov) if ov != null =>
        pbo.getState match {
          case PBFTStage.PRE_PREPARE =>
            StateStorage.updateNodeStage(pbo, PBFTStage.PREPARE)
            if (pbo.getRejectState != PBFTStage.REJECT) {
              inQ.offer((pbo, ov, PBFTStage.PREPARE));
              log.debug("Qsize=" + inQ.size())
            }

          case PBFTStage.PREPARE =>
            StateStorage.updateNodeStage(pbo, pbo.getState)
            if (pbo.getRejectState != PBFTStage.REJECT) {
              inQ.offer((pbo, ov, PBFTStage.COMMIT));
            }
          case PBFTStage.COMMIT =>
            StateStorage.updateNodeStage(pbo, pbo.getState)
            if (pbo.getRejectState != PBFTStage.REJECT) {
              inQ.offer((pbo, ov, PBFTStage.REPLY));
            }
          case PBFTStage.REPLY =>
            StateStorage.saveStageV(pbo, ov.build());
            log.info("MergeSuccess.Remote!:V=" + pbo.getV + ",N=" + pbo.getN + ",org=" + pbo.getOriginBcuid)
            PBFTStage.NOOP
          case _ =>
            PBFTStage.NOOP
        }

      case None =>
        inQ.offer((pbo, null, PBFTStage.REJECT));
        PBFTStage.REJECT
    }

  }

  def pollQ(): (PVBase, OValue.Builder, PBFTStage) = {
    inQ.poll(20, TimeUnit.SECONDS)
  }

  def main(args: Array[String]): Unit = {
    inQ.offer((PVBase.newBuilder().build(), null, PBFTStage.COMMIT))
    outQ.offer(PVBase.newBuilder().build())
    println(inQ.size())
    println(outQ.size())
  }

  def appendOutQ(pbo: PVBase) = {
    outQ.offer(pbo);
  }

}