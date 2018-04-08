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
import org.fc.brewchain.p22p.tasks.JoinNetwork
import org.fc.brewchain.p22p.tasks.CheckingHealthy
import org.fc.brewchain.p22p.tasks.VoteNodeMap
import org.fc.brewchain.p22p.pbft.VoteWorker
import java.util.concurrent.TimeUnit
import org.fc.brewchain.p22p.utils.Config
import org.fc.brewchain.p22p.tasks.Scheduler
import org.fc.brewchain.p22p.pbft.StateStorage
import java.util.HashMap
import org.fc.brewchain.p22p.pbft.VoteQueue

import scala.collection.JavaConversions._
import com.google.protobuf.ByteString
import org.fc.brewchain.p22p.node.router.FullNodeSet
import org.fc.brewchain.p22p.node.router.IntNode

case class BitEnc(bits: BigInt) {
  val strEnc: String = BitMap.hexToMapping(bits);
}
case class Network(netid: String, nodelist: String) extends OLog with LocalNode //
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

  val noneNode = PNode(_name = "NONE", _node_idx = 0, "",
    _try_node_idx = 0)

  def nodeByBcuid(name: String): PNode = directNodeByBcuid.getOrElse(name, noneNode);
  def nodeByIdx(idx: Int) = directNodeByIdx.get(idx);

  def directNodes: Iterable[PNode] = directNodeByBcuid.values

  def pendingNodes: Iterable[PNode] = pendingNodeByBcuid.values

  def addDNode(pnode: PNode): Option[PNode] = {

    val node = pnode.changeIdx(pnode.try_node_idx)
    this.synchronized {
      if (!directNodeByBcuid.contains(node.bcuid) && node.node_idx >= 0 && !node_bits().testBit(node.node_idx)) {
        if (StringUtils.equals(node.bcuid, root().bcuid)) {
          resetRoot(node)
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

  def inNetwork(): Boolean = {
    root().node_idx > 0 && nodeByIdx(root().node_idx()) != None;
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

  def wallMessage(gcmd: String, body: Either[Message, ByteString], messageId: String = "")(implicit nextHops: IntNode = FullNodeSet()): Unit = {
    if (circleNR.encbits.bitCount > 0) {
      //      log.debug("wall to DCircle:" + messageId + ",Dnodescount=" + directNodes.size + ",enc=" +
      //        node_strBits())
      circleNR.broadcastMessage(gcmd, body, from = root())(toN = root(), network = this, nextHops = nextHops, messageid = messageId)
    }
    pendingNodes.map(n =>
      {
        log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)(this)
      })
  }

  def bwallMessage(gcmd: String, body: Either[Message, ByteString], bits: BigInt, messageId: String = ""): Unit = {
    directNodes.map(n =>
      if (bits.testBit(n.node_idx)) {
        log.debug("bitpost to direct:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)(this)
      })
    pendingNodes.map(n =>
      if (bits.testBit(n.try_node_idx)) {
        log.debug("bitpost to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)(this)
      })
  }

  def dwallMessage(gcmd: String, body: Either[Message, ByteString], messageId: String = ""): Unit = {
    directNodes.map(n =>
      {
        log.debug("post to direct:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)(this)
      })
    pendingNodes.map(n =>
      {
        log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)(this)
      })
  }
  def wallOutsideMessage(gcmd: String, body: Either[Message, ByteString], messageId: String = ""): Unit = {
    directNodes.map { n =>
      if (!isLocal(n)) {
        log.debug("post to directNode:bcuid=" + n.bcuid + ",messageid=" + messageId);
        MessageSender.postMessage(gcmd, body, n)(this)
      }
    }
    pendingNodes.map(n =>
      {
        if (!isLocal(n)) {
          log.debug("post to pending:bcuid=" + n.bcuid + ",messageid=" + messageId);
          MessageSender.postMessage("VOTPZP", body, n)(this)
        }
      })
  }
  //
  val joinNetwork = JoinNetwork(this, nodelist);
  val stateStorage = StateStorage(this);
  val voteQueue = VoteQueue(this);

  def startup(): Unit = {
    Scheduler.scheduleWithFixedDelay(joinNetwork, 5, 10, TimeUnit.SECONDS)
    Scheduler.scheduleWithFixedDelay(CheckingHealthy(this), 10, Config.TICK_CHECK_HEALTHY, TimeUnit.SECONDS)
    Scheduler.scheduleWithFixedDelay(VoteNodeMap(this, voteQueue), 10, Config.TICK_VOTE_MAP, TimeUnit.SECONDS)
    Scheduler.scheduleWithFixedDelay(VoteWorker(this, voteQueue), 10, Config.TICK_VOTE_WORKER, TimeUnit.SECONDS)
  }
}
object Networks extends LogHelper {
  //  val raft: Network = new Network("raft","tcp://127.0.0.1:5100");
  val netsByID = new HashMap[String, Network]();
  def networkByID(netid: String): Network = {
    netsByID.get(netid);
  }
}



