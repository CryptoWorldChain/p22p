package org.fc.brewchain.p22p.node

import onight.tfw.mservice.NodeHelper
import org.fc.brewchain.bcapi.crypto.KeyPair
import org.fc.brewchain.bcapi.crypto.EncHelper
import org.apache.commons.lang3.StringUtils
//import org.spongycastle.util.encoders.Hex
//import org.ethereum.crypto.HashUtil
import org.fc.brewchain.p22p.Daos
import java.math.BigInteger
import onight.oapi.scala.traits.OLog
import com.google.protobuf.Message
import org.fc.brewchain.p22p.core.MessageSender
import org.fc.brewchain.bcapi.crypto.BCNodeHelper
import org.fc.brewchain.p22p.action.PMNodeHelper
import com.google.protobuf.StringValue
import com.google.protobuf.ByteString
import org.brewchain.bcapi.gens.Oentity.OKey
import org.slf4j.MDC
import org.fc.brewchain.p22p.utils.LogHelper

object NodeInstance extends OLog with PMNodeHelper with LogHelper {
  //  val node_name = NodeHelper.getCurrNodeName
  val NODE_ID_PROP = "org.bc.node.id"
  val PROP_NODE_INFO = "zp.bc.node.info";

  private var rootnode: PNode = null;

  def root(): PNode = rootnode;

  def getNodeIdx: Int = rootnode.node_idx

  def isLocalNode(node: PNode): Boolean = {
    node == root || root.bcuid.equals(node.bcuid)
  }

  def isLocalNode(bcuid: String): Boolean = {
    root.bcuid.equals(bcuid)
  }
  def getFromDB(key: String, defaultv: String): String = {
    val v = Daos.odb.get(OKey.newBuilder().setData(ByteString.copyFrom(key.getBytes)).build())
    if (v == null || v.get() == null) {
      val prop = NodeHelper.getPropInstance.get(key, defaultv);
      NodeHelper.envInEnv(prop)
    } else {
      v.get.getInfo
    }

  }

  def syncInfo(node: PNode): Boolean = {
    if (Daos.odb == null) return false;
    Daos.odb.putInfo(PROP_NODE_INFO, serialize(node));
    true
  }
  def isReady(): Boolean = {
    log.debug("check Node Instace:Daos.odb=" + Daos.odb)
    if (!Daos.isDbReady()) return false;
    if (rootnode == null)
      initNode()
    if (MessageSender.sockSender != null && rootnode != null && rootnode.bcuid != null) {
      MessageSender.sockSender.setCurrentNodeName(rootnode.bcuid)
    }
    rootnode != null;
  }
  def newNode(nodeidx: Int = -1): PNode = {
    val kp = EncHelper.newKeyPair()
    val newroot = PNode.signNode(NodeHelper.getCurrNodeName, node_idx = nodeidx, protocol = "tcp",
      address = NodeHelper.getCurrNodeListenOutAddr,
      NodeHelper.getCurrNodeListenOutPort,
      System.currentTimeMillis(), kp.pubkey,
      try_node_idx = nodeidx,
      bcuid = kp.bcuid,
      pri_key = kp.prikey)
    syncInfo(newroot)
    newroot;
  }
  def initNode() = {
    this.synchronized {
      if (rootnode == null) //second entry
      {
        try {
          val nodeidx = PNode.genIdx()
          val node_info = getFromDB(PROP_NODE_INFO, "");
          rootnode =
            try {
              log.info("load node from db info=:" + node_info)
              val r = if (StringUtils.isBlank(node_info)) {
                newNode(PNode.genIdx());
              } else {
                deserialize(node_info)
              }
              log.info("load node from db:" + r.bcuid + ",idx=" + r.node_idx)
              r
            } catch {
              case e: Throwable =>
                val r = newNode(PNode.genIdx());
                log.debug("new node info:" + r.bcuid + ",idx=" + r.node_idx)
                r
            }
        } catch {
          case e: Throwable =>
            log.warn("unknow Error.", e)
        } finally {
          if (NodeInstance.root() != null) {
            MDCSetBCUID()
          }
        }
      }
    }
  }
  def resetRoot(node: PNode): Unit = {
    this.rootnode = node;
  }
  def changeNodeIdx(test_bits: BigInt = BigInt("0")): Int = {
    this.synchronized {
      var v = 0;
      do {
        v = PNode.genIdx()
      } while (getNodeIdx == v || test_bits.testBit(v))

      Daos.odb.putInfo(NODE_ID_PROP, String.valueOf(v))
      rootnode = rootnode.changeIdx(v)
      MDCSetBCUID()
      syncInfo(rootnode)
      log.debug("changeNode Index=" + v)
      v
    }
  }

  def inNetwork(): Boolean = {
    getNodeIdx > 0 && Networks.instance.node_bits.bitCount >= 2;
  }

  def isLocal(bcuid: String): Boolean = {
    StringUtils.equals(root().bcuid, bcuid);
  }

  def isLocal(node: PNode): Boolean = {
    node == root() ||
      StringUtils.equals(root().bcuid, node.bcuid);
  }

}
