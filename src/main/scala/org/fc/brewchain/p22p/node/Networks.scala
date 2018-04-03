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

class Network() extends OLog //
{
  val directNodeByBcuid: Map[String, PNode] = Map.empty[String, PNode];
  val directNodeByIdx: Map[Int, PNode] = Map.empty[Int, PNode];
  val pendingNodeByBcuid: Map[String, PNode] = Map.empty[String, PNode];

  val connectedMap: Map[Int, Map[Int, Int]] = Map.empty[Int, Map[Int, Int]];

  var node_bits = BigInt(0)
  def nodeByBcuid(name: String): Node = directNodeByBcuid.getOrElse(name, NoneNode());
  def nodeByIdx(idx: Int) = directNodeByIdx.get(idx);

  def directNodes: Iterable[PNode] = directNodeByBcuid.values

  def pendingNodes: Iterable[PNode] = pendingNodeByBcuid.values

  def addDNode(pnode: PNode): Option[PNode] = {
    val node = pnode.changeIdx(pnode.try_node_idx)
    if (!directNodeByBcuid.contains(node.bcuid) && node.node_idx >= 0 && !node_bits.testBit(node.node_idx)) {
      if (StringUtils.equals(node.bcuid, NodeInstance.root().bcuid)) {
        NodeInstance.resetRoot(node)
      }
      directNodeByBcuid.put(node.bcuid, node)
      node_bits = node_bits.setBit(node.node_idx);
      directNodeByIdx.put(node.node_idx, node);
      Some(node)
    } else {
      None
    }
  }

  def removeDNode(node: PNode): Option[PNode] = {
    if (directNodeByBcuid.contains(node.bcuid)) {
      node_bits = node_bits.clearBit(node.node_idx);
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
        log.debug("pendingNode exists PendingNodes bcuid=" + node.bcuid);
        false
      } else {
        pendingNodeByBcuid.put(node.bcuid, node);
        log.debug("addpending:" + pendingNodeByBcuid.size + ",p=" + node.bcuid)
        true
      }
    }
  }
  def pending2DirectNode(nodes: List[PNode], checkBits: BigInt): Boolean = {
    this.synchronized {
      nodes.map { node =>
        removePendingNode(node)
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
}
object Networks extends LogHelper {
  val instance: Network = new Network();
  def forwardMessage(fp: FramePacket) {
    CircleNR.broadcastMessage(fp)(NodeInstance.root(), network = instance)
  }
  def wallMessage(gcmd: String, body: Message, messageId: String = ""): Unit = {
    instance.directNodes.map { n =>
      log.debug("post to directNode:bcuid=" + n.bcuid + ",messageid=" + messageId);
      MessageSender.postMessage(gcmd, body, n);
    }
    instance.pendingNodes.map(n =>
      {
        log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage("VOTPZP", body, n)
      })
  }
}



