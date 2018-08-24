package org.fc.brewchain.p22p.tasks

import java.util.concurrent.TimeUnit
import onight.oapi.scala.traits.OLog
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
import java.util.concurrent.ConcurrentHashMap
import org.fc.brewchain.p22p.node.Network
import sun.rmi.log.LogHandler
import org.fc.brewchain.p22p.utils.LogHelper
import java.util.concurrent.CountDownLatch
import org.brewchain.bcapi.exec.SRunner

//投票决定当前的节点
case class JoinNetwork(network: Network, statupNodes: Iterable[PNode]) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "JoinNetwork"
  val sameNodes = new HashMap[Integer, PNode]();
  val pendingJoinNodes = new ConcurrentHashMap[String, PNode]();
  val joinedNodes = new HashMap[Integer, PNode]();
  val duplictedInfoNodes = Map[Int, PNode]();
  def runOnce() = {
    Thread.currentThread().setName("JoinNetwork");

    implicit val _net = network
    if (network.inNetwork()&&pendingJoinNodes.size()<=0) {
      log.debug("CurrentNode In Network");
    } else {
      var hasNewNode = true;
      var joinLoopCount = 0;
      while (hasNewNode && joinLoopCount < 3) {
        try {
          val failedNodes = new HashMap[String, PNode]();
          hasNewNode = false;
          joinLoopCount = joinLoopCount + 1;
          MDCSetBCUID(network);
          val namedNodes = (statupNodes ++ pendingJoinNodes.values()).filter { x =>
            StringUtils.isNotBlank(x.uri) && !sameNodes.containsKey(x.uri.hashCode()) && !joinedNodes.containsKey(x.uri.hashCode()) && //
              !network.isLocalNode(x)
          };

          val cdl = new CountDownLatch(namedNodes.size);
          namedNodes.map { n => //for each know Nodes
            //          val n = namedNodes(0);
            log.debug("JoinNetwork :Run----Try to Join :MainNet=" + n.uri + ",M.bcuid=" + n.bcuid() + ",cur=" + network.root.uri);
            if (!network.root.equals(n)) {
              val joinbody = PSJoin.newBuilder().setOp(PSJoin.Operation.NODE_CONNECT).setMyInfo(toPMNode(network.root()))
                .setNid(network.netid)
                .setNetworkInstance(Networks.instanceid)
                .setNodeCount(network.pendingNodeByBcuid.size
                  + network.directNodeByBcuid.size)
                .setNodeNotifiedCount(joinedNodes.size());
              val starttime = System.currentTimeMillis();

              log.debug("JoinNetwork :Start to Connect---:" + n.uri);

              MessageSender.asendMessage("JINPZP", joinbody.build(), n, new CallBack[FramePacket] {
                def onSuccess(fp: FramePacket) = {
                  MDCSetBCUID(network);
                  cdl.countDown()
                  log.debug("send JINPZP success:to " + n.uri + ",cost=" + (System.currentTimeMillis() - starttime))
                  val retjoin = PRetJoin.newBuilder().mergeFrom(fp.getBody);
                  if (retjoin.getRetCode() == -1) { //same message
                    log.debug("get Same Node:" + n.getName);
                    sameNodes.put(n.uri.hashCode(), n);
                    val newN = fromPMNode(retjoin.getMyInfo)
                    MessageSender.changeNodeName(n.bcuid, newN.bcuid);
                    MessageSender.dropNode(newN)
                    network.onlineMap.put(newN.bcuid(), newN)
                    network.addPendingNode(newN);
                  } else if (retjoin.getRetCode() == -2) {
                    log.debug("get duplex NodeIndex:" + n.getName);
                    sameNodes.put(n.uri.hashCode(), n);
                    duplictedInfoNodes.+=(n.uri.hashCode() -> n);
                  } else if (retjoin.getRetCode() == 0) {
                    joinedNodes.put(n.uri.hashCode(), n);
                    val newN = fromPMNode(retjoin.getMyInfo)
                    MessageSender.changeNodeName(n.bcuid, newN.bcuid);
                    network.addPendingNode(newN);
                    network.onlineMap.put(newN.bcuid(), newN)
                    retjoin.getNodesList.map { node =>
                      val pnode = fromPMNode(node);
                      if (network.addPendingNode(pnode)) {
                        pendingJoinNodes.put(node.getBcuid, pnode);
                      }
                      //
                    }
                  }
                  log.debug("get nodes:count=" + retjoin.getNodesCount + "," + sameNodes);
                }
                def onFailed(e: java.lang.Exception, fp: FramePacket) {
                  failedNodes.put(n.uri, n);
                  log.debug("send JINPZP ERROR " + n.uri + ",e=" + e.getMessage, e)
                  cdl.countDown()
                }
              });
            } else {
              cdl.countDown()
              log.debug("JoinNetwork :Finished ---- Current node is MainNode");
            }
            try {
              cdl.await(60, TimeUnit.SECONDS);
            } catch {
              case t: Throwable =>
                log.debug("error connect to all nodes:" + t.getMessage, t);
            } finally {

            }
            log.debug("finished connect to all nodes");
          }
          if (namedNodes.size == 0) {
            log.debug("cannot reach more nodes. try from begining");
            if (duplictedInfoNodes.size > network.pendingNodes.size / 3 && !network.directNodeByBcuid.contains(network.root().bcuid)) {
              //            val nl = duplictedInfoNodes.values.toSeq.PBFTVote { x => Some(x.node_idx) }
              //            nl.decision match {
              //              case Some(v: BigInteger) =>
              log.debug("duplictedInfoNodes ,change My Index:" + duplictedInfoNodes.size);
              network.removePendingNode(network.root())

              network.changeNodeIdx(duplictedInfoNodes.head._2.node_idx);
              //drop all connection first
              pendingJoinNodes.clear()
              joinedNodes.clear();
              sameNodes.clear();
              //              case _ => {
              //                log.debug("cannot get Converage :" + nl);
              //network.changeNodeIdx();
              //              }
              //            }
              //} else {
              //network.changeNodeIdx();
            }
            //          joinedNodes.clear();
            duplictedInfoNodes.clear();
            //next run try another index;
          } else {
            if (pendingJoinNodes.size() > 0) {
              if (pendingJoinNodes.filter(p => !failedNodes.containsKey(p._2.uri())).size > 0) {
                hasNewNode = true;
              }
            }
          }
        } catch {
          case e: Throwable =>
            log.debug("JoinNetwork :Error", e);
        } finally {
          log.debug("JoinNetwork :[END]")
        }
      }
    }
  }
}