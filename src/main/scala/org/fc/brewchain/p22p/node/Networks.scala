package org.fc.brewchain.p22p.node

import org.fc.brewchain.p22p.exception.NodeInfoDuplicated
import java.net.URL
import org.apache.commons.lang3.StringUtils
import onight.tfw.mservice.NodeHelper
import com.google.protobuf.MessageOrBuilder
import org.fc.brewchain.p22p.core.MessageSender
import com.google.protobuf.Message
import scala.collection.mutable.Map
import onight.oapi.scala.traits.OLog
import scala.collection.Iterable
import onight.tfw.otransio.api.beans.FramePacket
import org.fc.brewchain.p22p.node.router.CircleNR
import org.fc.brewchain.p22p.utils.LogHelper
import org.fc.brewchain.bcapi.crypto.BitMap

case class BitEnc(bits: BigInt) {
  val strEnc: String = BitMap.hexToMapping(bits);
}
class Network() extends OLog //
{
  val directNodeByBcuid: Map[String, PNode] = Map.empty[String, PNode];
  val directNodeByIdx: Map[Int, PNode] = Map.empty[Int, PNode];
  val pendingNodeByBcuid: Map[String, PNode] = Map.empty[String, PNode];
  val connectedMap: Map[Int, Map[Int, Int]] = Map.empty[Int, Map[Int, Int]];
  val onlineMap: Map[String, PNode] = Map.empty[String, PNode];

  //  var _node_bits = BigInt(0)
  var bitenc = BitEnc(BigInt(0))

  def node_bits(): BigInt = bitenc.bits
  def node_strBits(): String = bitenc.strEnc;

  def resetNodeBits(_new: BigInt): Unit = {
    this.synchronized {
      bitenc = BitEnc(_new)
      circleNR = CircleNR(_new)
    }
  }

  var circleNR: CircleNR = CircleNR(node_bits())

  val noneNode = PNode(name = "NONE", node_idx = 0, "",
    try_node_idx = 0)
  def nodeByBcuid(name: String): PNode = directNodeByBcuid.getOrElse(name, noneNode);
  def nodeByIdx(idx: Int) = directNodeByIdx.get(idx);

  def directNodes: Iterable[PNode] = directNodeByBcuid.values

  def pendingNodes: Iterable[PNode] = pendingNodeByBcuid.values

  def addDNode(pnode: PNode): Option[PNode] = {

    val node = pnode.changeIdx(pnode.try_node_idx)
    this.synchronized {
      if (!directNodeByBcuid.contains(node.bcuid) && node.node_idx >= 0 && !node_bits().testBit(node.node_idx)) {
        if (StringUtils.equals(node.bcuid, NodeInstance.root().bcuid)) {
          NodeInstance.resetRoot(node)
        }
        directNodeByBcuid.put(node.bcuid, node)
        resetNodeBits(node_bits.setBit(node.node_idx));

        directNodeByIdx.put(node.node_idx, node);
        removePendingNode(node)
        Some(node)
      } else {
        None
      }
    }
  }

  def removeDNode(node: PNode): Option[PNode] = {
    if (directNodeByBcuid.contains(node.bcuid)) {
      resetNodeBits(node_bits.clearBit(node.node_idx));
      directNodeByBcuid.remove(node.bcuid)
    } else {
      None
    }
  }
  //  var node_idx = _node_idx; //全网确定之后的节点id

  def addPendingNode(node: PNode): Boolean = {
    this.synchronized {
      if (directNodeByBcuid.contains(node.bcuid)) {
        log.debug("directNode exists in DirectNode bcuid=" + node.bcuid);
        false
      } else if (pendingNodeByBcuid.contains(node.bcuid)) {
        //        log.debug("pendingNode exists PendingNodes bcuid=" + node.bcuid);
        false
      } else {
        pendingNodeByBcuid.put(node.bcuid, node);
        log.debug("addpending:" + pendingNodeByBcuid.size + ",p=" + node.bcuid)
        true
      }
    }
  }
  def pending2DirectNode(nodes: List[PNode]): Boolean = {
    this.synchronized {
      nodes.map { node =>
        addDNode(node)
      }.filter { _ == None }.size == 0
    }
  }

  def removePendingNode(node: PNode): Boolean = {
    this.synchronized {
      if (!pendingNodeByBcuid.contains(node.bcuid)) {
        false
      } else {
        pendingNodeByBcuid.remove(node.bcuid);
        log.debug("remove:" + pendingNodeByBcuid.size + ",p=" + node.bcuid)
        true
      }
    }
  }

  def updateConnect(fromIdx: Int, toIdx: Int) = {
    if (fromIdx != -1 && toIdx != -1)
      connectedMap.synchronized {
        connectedMap.get(fromIdx) match {
          case Some(m) =>
            m.put(toIdx, fromIdx)
          case None =>
            connectedMap.put(fromIdx, scala.collection.mutable.Map[Int, Int](toIdx -> fromIdx))
        }
        connectedMap.get(toIdx) match {
          case Some(m) =>
            m.put(fromIdx, toIdx)
          case None =>
            connectedMap.put(toIdx, scala.collection.mutable.Map[Int, Int](fromIdx -> toIdx))
        }
        //      log.debug("map="+connectedMap)
      }
  }

  def wallMessage(gcmd: String, body: Message, messageId: String = ""): Unit = {
    if (circleNR.encbits.bitCount > 0) {
      log.debug("wall to direct:" + messageId + ",dnodescount=" + directNodes.size + ",enc=" +
        node_strBits())
      circleNR.broadcastMessage(gcmd, body)(network = this)
    }
    pendingNodes.map(n =>
      {
        log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)
      })
  }

  def bwallMessage(gcmd: String, body: Message, bits: BigInt, messageId: String = ""): Unit = {
    directNodes.map(n =>
      if (bits.testBit(n.node_idx)) {
        log.debug("bitpost to direct:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)
      })
    pendingNodes.map(n =>
      if (bits.testBit(n.try_node_idx)) {
        log.debug("bitpost to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)
      })
  }

  def dwallMessage(gcmd: String, body: Message, messageId: String = ""): Unit = {
    directNodes.map(n =>
      {
        log.debug("post to direct:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)
      })
    pendingNodes.map(n =>
      {
        log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)
      })
  }
}
object Networks extends LogHelper {
  val instance: Network = new Network();
  //  def forwardMessage(fp: FramePacket) {
  ////    CircleNR.broadcastMessage(fp)(NodeInstance.root(), network = instance)
  //  }
  def wallMessage(gcmd: String, body: Message, messageId: String = ""): Unit = {
    instance.wallMessage(gcmd, body, messageId);
  }

  def dwallMessage(gcmd: String, body: Message, messageId: String = ""): Unit = {
    instance.dwallMessage(gcmd, body, messageId);
  }
  def wallOutsideMessage(gcmd: String, body: Message, messageId: String = ""): Unit = {
    instance.directNodes.map { n =>
      if (!NodeInstance.isLocal(n)) {
        log.debug("post to directNode:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n);
      }
    }
    instance.pendingNodes.map(n =>
      {
        if (!NodeInstance.isLocal(n)) {
          log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
          MessageSender.postMessage("VOTPZP", body, n)
        }
      })
  }
}



