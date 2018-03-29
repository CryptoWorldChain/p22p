package org.fc.brewchain.p22p.tasks

import java.util.concurrent.TimeUnit
import onight.oapi.scala.traits.OLog
import org.fc.brewchain.p22p.node.NodeInstance
import org.fc.brewchain.p22p.pbgens.P22P.PMNodeInfo
import org.fc.brewchain.p22p.pbgens.P22P.PBVoteNodeIdx
import java.math.BigInteger
import scala.collection.JavaConverters
import org.fc.brewchain.p22p.node.PNode
import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.p22p.core.MessageSender
import org.fc.brewchain.p22p.pbgens.P22P.PSJoin
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CallBack
import org.fc.brewchain.p22p.pbgens.P22P.PRetJoin
import org.fc.brewchain.p22p.Daos
import java.net.URL
import org.fc.brewchain.bcapi.URLHelper
import org.osgi.service.url.URLStreamHandlerService
import onight.tfw.otransio.api.PacketHelper
import scala.collection.mutable.ArrayBuffer
import java.util.HashMap
import org.fc.brewchain.p22p.core.Votes._
import scala.collection.mutable.Map
import org.fc.brewchain.p22p.action.PMNodeHelper
import org.fc.brewchain.p22p.node.Networks
import scala.collection.JavaConversions._

//投票决定当前的节点
object JoinNetwork extends SRunner {
  def getName() = "JoinNetwork"
  val sameNodes = new HashMap[Integer, PNode]();
  val joinedNodes = new HashMap[Integer, PNode]();
  val duplictedInfoNodes = Map[Int, PNode]();
  def runOnce() = {
    Thread.currentThread().setName("JoinNetwork");
    if (NodeInstance.inNetwork()) {
      log.debug("CurrentNode In Network");
    } else {
      try {
        val namedNodes = Daos.props.get("org.bc.networks", "tcp://localhost:5100").split(",").map { x =>
          log.debug("x=" + x)
          PNode.fromURL(x);
        }.filter { x =>
          !sameNodes.containsKey(x.uri.hashCode()) && !joinedNodes.containsKey(x.uri.hashCode()) && //
            !NodeInstance.isLocalNode(x)
        };
        namedNodes.map { n => //for each know Nodes
          //          val n = namedNodes(0);
          log.debug("JoinNetwork :Run----Try to Join :MainNet=" + n.uri + ",cur=" + NodeInstance.root.uri);
          if (!NodeInstance.root.equals(n)) {
            val joinbody = PSJoin.newBuilder().setOp(PSJoin.Operation.NODE_CONNECT).setMyInfo(toPMNode());
            log.debug("JoinNetwork :Start to Connect---:" + n.uri);
            MessageSender.sendMessage("JINPZP", joinbody.build(), n, new CallBack[FramePacket] {
              def onSuccess(fp: FramePacket) = {
                log.debug("send JINPZP success:to " + n.uri + ",body=" + fp.getBody)
                val retjoin = PRetJoin.newBuilder().mergeFrom(fp.getBody);
                if (retjoin.getRetCode() == -1) { //same message
                  log.debug("get Same Node:" + n);
                  sameNodes.put(n.uri.hashCode(), n);
                  duplictedInfoNodes.+=(n.uri.hashCode() -> n);
                  MessageSender.dropNode(n)
                } else if (retjoin.getRetCode() == 0) {
                  joinedNodes.put(n.uri.hashCode(), n);
                  val newN = fromPMNode(retjoin.getMyInfo)
                  MessageSender.changeNodeName(n.bcuid, newN.bcuid);
                  Networks.instance.addPendingNode(newN);
                  retjoin.getNodesList.map { node =>
                    Networks.instance.addPendingNode(fromPMNode(node))
                    //
                  }
                }
                log.debug("get nodes:" + retjoin);
              }
              def onFailed(e: java.lang.Exception, fp: FramePacket) {
                log.debug("send JINPZP ERROR " + n.uri + ",e=" + e.getMessage, e)
              }
            });
          } else {
            log.debug("JoinNetwork :Finished ---- Current node is MainNode");
          }
        }
        if (namedNodes.size == 0) {
          log.debug("cannot reach more nodes. try from begining");
          if (duplictedInfoNodes.size > 0) {
            val nl = duplictedInfoNodes.values.toSeq.PBFTVote { x => Some(x.node_idx) }
            nl.decision match {
              case Some(v: BigInteger) =>
                log.debug("get Converage Value:" + v);
                Networks.instance.removePendingNode(NodeInstance.root())
                NodeInstance.changeNodeIdx(v);
              case _ => {
                log.debug("cannot get Converage :" + nl);
                //NodeInstance.changeNodeIdx();
              }
            }
            //} else {
            //NodeInstance.changeNodeIdx();
          }
          //          joinedNodes.clear();
          duplictedInfoNodes.clear();
          //next run try another index;
        }
      } catch {
        case e: Throwable =>
          log.debug("JoinNetwork :Error", e);
      } finally {
        log.debug("JoinNetwork :[END]")
      }
    }
  }
  //Scheduler.scheduleWithFixedDelay(new Runnable, initialDelay, delay, unit)
}