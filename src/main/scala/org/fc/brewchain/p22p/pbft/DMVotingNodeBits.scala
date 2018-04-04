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
import org.fc.brewchain.p22p.node.Networks
import org.fc.brewchain.p22p.action.PMNodeHelper
import org.fc.brewchain.bcapi.crypto.BitMap
import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.p22p.node.NodeInstance

object DMVotingNodeBits extends Votable with OLog with PMNodeHelper {
  def makeDecision(pbo: PVBase, reallist: List[OPair]): Option[String] = {
    val vb = PBVoteNodeIdx.newBuilder().mergeFrom(pbo.getContents);
    log.debug("makeDecision NodeBits:F=" + pbo.getFromBcuid + ",R=" + vb.getNodeBitsEnc + ",S=" + pbo.getState
      + ",V=" + pbo.getV + ",J=" + pbo.getRejectState);
    if (pbo.getRejectState == PBFTStage.REJECT) {
      None;
    } else {
      val encbits = BitMap.mapToBigInt(vb.getNodeBitsEnc);
      val pendingbits = BitMap.mapToBigInt(vb.getPendingBitsEnc);
      val oldtotalbits = encbits.+(pendingbits);
      var totalbits = encbits.+(pendingbits);

      val pendingInList = vb.getPendingNodesList.filter { pn =>
        pn.getBcuid.equals(NodeInstance.root().bcuid) ||
          Networks.instance.onlineMap.contains(pn.getBcuid) ||
          Networks.instance.directNodeByBcuid.contains(pn.getBcuid)
      }
      totalbits = totalbits.clearBit(NodeInstance.root().try_node_idx)
      Networks.instance.directNodes.map { pn =>
        totalbits = totalbits.clearBit(pn.node_idx)
      }
      Networks.instance.pendingNodes.map { pn =>
        //      if(!Networks.instance.onlineMap.contains(pn.bcuid)){
        //        log.warn("pending node not online:"+pn.bcuid);
        //      }
        totalbits = totalbits.clearBit(pn.try_node_idx)
      }
      log.debug("totalbits::" + oldtotalbits.toString(16) + "-->" + totalbits.toString(16)
        + ":pbpendinCount=" + vb.getPendingNodesCount
        + ":pendingInList::" + pendingInList.foldLeft(",")((A, p) => A + p.getBcuid + ","))

      //1. check encbits. for direct nodes 
      if (pendingInList.size == vb.getPendingNodesCount
        && totalbits.bitCount == 0) {
        Some(vb.getNodeBitsEnc)
      } else {
        log.debug("reject for node_bits not equals:")
        None;
      }
    }
  }
  def finalConverge(pbo: PVBase): Unit = {
    val vb = PBVoteNodeIdx.newBuilder().mergeFrom(pbo.getContents);
    log.debug("FinalConverge! for DMVotingNodeBits:F=" + pbo.getFromBcuid + ",Result=" + vb.getNodeBitsEnc);

    val nodes = vb.getPendingNodesList.map { n =>
      fromPMNode(n)
    }.toList

    val encbits = BitMap.mapToBigInt(vb.getNodeBitsEnc);
    val hasBitExistsInMypending = Networks.instance.pendingNodes.map { n =>
      if (encbits.testBit(n.try_node_idx)) {
        Networks.instance.addDNode(n);
      }
    }
    Networks.instance.pending2DirectNode(nodes) match {
      case true =>
        log.info("success add pending to direct nodes::" + nodes.size)
      case false =>
        log.warn("failed add pending to direct nodes::" + nodes.size)
    }

  }
}
