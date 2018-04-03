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
    log.debug("makeDecision for DMVotingNodeBits:F=" + pbo.getFromBcuid + ",R=" + vb.getNodeBitsEnc);
    val pendingbits = BitMap.mapToBigInt(vb.getPendingBitsEnc);
    if (pendingbits.testBit(NodeInstance.root().try_node_idx)||
        StringUtils.equals(vb.getNodeBitsEnc, BitMap.hexToMapping(Networks.instance.node_bits))) {
      val hasBitExists = vb.getPendingNodesList.foldLeft(false)((R, n) => {
        if (R || Networks.instance.node_bits.testBit(n.getTryNodeIdx)) {
          log.debug("pending nodes idx conflict:" + n.getBcuid + ",tryidx=" + n.getTryNodeIdx)
          true
        } else {
          R
        }
      })
      if (hasBitExists) {
        log.debug("reject for bits exist:")
        None;
      } else {
        Some(vb.getNodeBitsEnc)
      }
    } else {
      log.debug("reject for node_bits not equals:")
      None
    }
  }
  def finalConverge(pbo: PVBase): Unit = {
    val vb = PBVoteNodeIdx.newBuilder().mergeFrom(pbo.getContents);
    log.debug("FinalConverge! for DMVotingNodeBits:F=" + pbo.getFromBcuid + ",Result=" + vb.getNodeBitsEnc);
    var bits = BigInt(0);

    val nodes = vb.getPendingNodesList.map { n =>
      if (bits.testBit(n.getTryNodeIdx)) {
        log.debug("error in try_node_idx @n=" + n.getBcuid + ",try=" + n.getTryNodeIdx + ",bits=" + bits);
      } else { //no pub keys
        bits = bits.setBit(n.getTryNodeIdx);
      }
      fromPMNode(n)
    }.toList
    
    val bitsstr = BitMap.hexToMapping(bits);
    if (!StringUtils.equals(bitsstr, vb.getPendingBitsEnc)) {
      log.warn("bits error: pending local.bitenc:" + bitsstr + ",pbo.bitenc=" + vb.getPendingBitsEnc)
    } else {
      Networks.instance.pending2DirectNode(nodes, bits) match {
        case true =>
          log.info("success add pending to direct nodes")
        case false =>
          log.warn("failed add pending to direct nodes")
      }
    }
  }
}
