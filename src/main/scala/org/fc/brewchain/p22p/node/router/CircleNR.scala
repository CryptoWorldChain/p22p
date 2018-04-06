package org.fc.brewchain.p22p.node.router

import scala.collection.mutable.Set
import scala.collection.Map
import org.apache.commons.lang3.StringUtils
import onight.oapi.scala.traits.OLog
import scala.concurrent.blocking
import onight.tfw.otransio.api.beans.FramePacket
import org.fc.brewchain.p22p.node.PNode
import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.node.Networks
import org.fc.brewchain.p22p.node.NodeInstance
import java.util.HashMap
import com.google.protobuf.Message
import org.fc.brewchain.p22p.core.MessageSender
import org.fc.brewchain.p22p.pbgens.P22P.PSRouteMessage
import org.fc.brewchain.bcapi.crypto.BitMap
import org.fc.brewchain.p22p.utils.NodeSetHelper

case class CMSetInfo(nodecount: Int, circleMap: Map[Int, Set[Int]])

case class CircleNR(encbits: BigInt) extends MessageRouter with OLog  with NodeSetHelper{
  def getRand() = DHTConsRand.getRandFactor()
  val ncount = encbits.bitCount;
  val cminfo: CMSetInfo = CMSetInfo(ncount, CMSCalc.markCircleSets(ncount).toMap)
  val idxMap: Map[Int, Int] = {
    val ret = scala.collection.mutable.Map[Int, Int]();
    for (i <- 0 to encbits.bitLength) {
      if (encbits.testBit(i)) {
        ret.put(ret.size, i);
      }
    }
    ret
  }
  
 val idxMapRemap: Map[Int, Int] = {
    val ret = scala.collection.mutable.Map[Int, Int]();
    for (i <- 0 to encbits.bitLength) {
      if (encbits.testBit(i)) {
        ret.put(i,ret.size);
      }
    }
    ret
  }
  override def broadcastMessage(gcmd:String,body: Message, from: PNode = NodeInstance.root)(implicit toN: PNode = NodeInstance.root,
    nextHops: IntNode = FullNodeSet(),
    network: Network = Networks.instance): Unit = {
    
//    log.debug("broadcastMessage:cur=@" + toN.node_idx + ",from.idx=" + from.node_idx + ",netxt=" + nextHops)
    toN.counter.recv.incrementAndGet();
    //    network.updateConnect(from.node_idx, to.node_idx)
    toN.processMessage(gcmd,body) 
    nextHops match {
      case f: FullNodeSet =>
        //from begin
        val (treere, result) = CMSCalc.calcRouteSets(idxMapRemap.get(toN.node_idx).get)(cminfo.circleMap)
//                log.debug("CMSCalc:" + idxMap)
        idxMap.get(treere.fromIdx) match {
          case Some(idx) =>
            network.nodeByIdx(idx) match {
              case Some(n) =>
                treere.treeHops.nodes.map { nids =>
                  routeMessage(gcmd,body)(n, nids, network)
                }
              case _ =>
                log.warn("not found id:" + treere.fromIdx + "==>idx=" + idx)
            }
          case _ =>
            log.warn("not found Map:treeidx=" + treere.fromIdx)
        }
      case none: EmptySet =>
        log.debug("Leaf Node");
      case ns: NodeSet =>
        ns.nodes.map { nids =>
          routeMessage(gcmd,body)(toN, nids, network)
        }
      case subset: DeepTreeSet =>
        routeMessage(gcmd,body)(toN, subset, network)
      case subset:IntNode =>
        log.warn("unknow subset:"+subset)
    }

  }
  override def routeMessage(gcmd:String,body: Message)(implicit from: PNode, //
    nextHops: IntNode,
    network: Network) {
//        log.debug("routeMessage:from=" + from.node_idx + ",next=" + nextHops)
    from.counter.send.incrementAndGet()
    nextHops match {
      case ts: DeepTreeSet =>
        //        log.debug(" route :DeepTreeSet:" + ts)
        idxMap.get(ts.fromIdx) match {
          case Some(idx) =>
            // log.debug("getnodeidx=="+idx)
            network.nodeByIdx(idx) match {
              case Some(n) =>
//                here to to route to others
                log.debug("route to other:"+n.bcuid);
                val vbody = PSRouteMessage.newBuilder().setBody(body.toByteString())
                .setEncbits(network.node_strBits())
                .setFromIdx(from.node_idx)
                .setGcmd(gcmd)
                .setNetwork("local").setNextHops(scala2pb(ts.treeHops)).build();
                MessageSender.postMessage("RRRPZP", vbody,n);
//                broadcastMessage(gcmd,body, from)(n, ts.treeHops, network)
              case _ =>
                log.warn("not found id:" + ts.fromIdx + "==>idx=" + idx)
            }
          case _ =>
            log.warn("not found Map:treeidx=" + ts.fromIdx)

        }

      case _ =>
        log.warn("unknow Set," + nextHops)
    }

  }

}