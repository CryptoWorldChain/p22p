package org.fc.brewchain.p22p.node

import onight.oapi.scala.traits.OLog
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.Set
import java.math.BigInteger
import org.fc.brewchain.p22p.exception.NodeInfoDuplicated
import onight.tfw.mservice.NodeHelper
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.Message
import java.util.concurrent.atomic.AtomicBoolean
import org.fc.brewchain.p22p.stat.MessageCounter
import org.apache.commons.lang3.StringUtils
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import org.fc.brewchain.p22p.stat.MessageCounter.CCSet
import org.fc.brewchain.p22p.node.router.RandomNR
import onight.tfw.otransio.api.beans.FramePacket

import scala.concurrent.blocking
import java.net.URL
import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.outils.conf.PropHelper
import org.apache.commons.codec.binary.Base64
import org.fc.brewchain.bcapi.crypto.EncHelper
import org.fc.brewchain.p22p.core.MessageSender
import scala.util.Either
import com.google.protobuf.ByteString

sealed trait Node {
  def processMessage(gcmd: String, body: Either[Message,ByteString])(implicit network: Network): Unit
  def changeIdx(idx: Int): Node;
  def name: String
  def node_idx: Int;
  def bcuid: String;
  def pub_key: String;
  def pri_key: String;
  def counter: CCSet;
  def startup_time: Long;
  def sign: String;
  def uri: String;
  def uris: Array[String];
  def try_node_idx: Int;

}

case class PNode(_name: String, _node_idx: Int, //node info
    _sign: String,
    protocol: String = "", address: String = "", port: Int = 0, //
    _startup_time: Long = System.currentTimeMillis(), //
    _pub_key: String = null, //
    _counter: CCSet = CCSet(),
    _try_node_idx: Int = 0,
    _bcuid: String = UUIDGenerator.generate(),
    _pri_key: String = null) extends Node with OLog {

  def uri(): String = protocol + "://" + address + ":" + port;
  def uris(): Array[String] = Array(protocol + "://" + address + ":" + port);

  def name(): String = _name
  def node_idx(): Int = _node_idx;
  def bcuid(): String = _bcuid;
  def pub_key(): String = _pub_key;
  def pri_key(): String = _pri_key
  def counter(): CCSet = _counter
  def startup_time(): Long = _startup_time
  def sign(): String = _sign
  def try_node_idx(): Int = _try_node_idx

  override def processMessage(gcmd: String, body: Either[Message,ByteString])(implicit network: Network): Unit = {
    //    counter.recv.incrementAndGet() 
    log.debug("procMessage:@" + node_idx + ",bcuid=" + bcuid + ",gcmd:" + gcmd)
    MessageSender.postMessage(gcmd, body, this)
  }

  override def toString(): String = {
    "PNode(" + uri + "," + startup_time + "," + pub_key + "," + node_idx + "," + sign + ")@" + this.hashCode()
  }

  override def changeIdx(idx: Int): PNode = PNode.signNode(
    name, idx, //node info
    protocol, address, port, //
    startup_time, //
    pub_key, //
    counter,
    try_node_idx,
    bcuid,
    pri_key)
}

object PNode {
  def fromURL(url: String): PNode = {
    val u = new URL(url);
    val n = new PNode(_name = u.getHost, _node_idx = 0, "", protocol = u.getProtocol, address = u.getHost, port = u.getPort,
      _bcuid = Base64.encodeBase64URLSafeString(url.getBytes))
    n
  }

  def NoneNode: PNode = PNode(_name = "", _node_idx = 0, _sign = "")

  def signNode(name: String, node_idx: Int, //node info
    protocol: String = "", address: String = "", port: Int = 0, //
    startup_time: Long = System.currentTimeMillis(), //
    pub_key: String = null, //
    counter: CCSet = CCSet(),
    try_node_idx: Int = 0,
    bcuid: String = UUIDGenerator.generate(),
    pri_key: String = null): PNode = {
    if (pri_key != null) {
      PNode(name, node_idx, EncHelper.ecSign(pri_key, Array(node_idx, protocol, address, port, bcuid).mkString("|").getBytes),
        protocol, address, port, //
        startup_time, //
        pub_key, //
        counter,
        try_node_idx,
        bcuid,
        pri_key)
    } else {
      PNode(name, node_idx, null,
        protocol, address, port, //
        startup_time, //
        pub_key, //
        counter,
        try_node_idx,
        bcuid,
        pri_key)
    }
  }

  val prop: PropHelper = new PropHelper(null);

  def genIdx(newidx: Int = -1): Int = {
    var currentidx: Int = newidx
    if (currentidx == -1) {
      currentidx = (Math.abs(Math.random() * 100000 % prop.get("otrans.node.max_nodes", 256))).asInstanceOf[Int];
    }
    val d = prop.get("otrans.node.idx", "" + currentidx);
    val envid = System.getProperty("otrans.node.idx", d);
    try {
      Integer.parseInt(NodeHelper.envInEnv(envid));
    } catch {
      case _: Throwable =>
        currentidx
    }
  }
}

case class ClusterNode(net_id: String, cnode_idx: Int, //node info
    _sign: String,
    pnodes: Array[PNode],
    _counter: CCSet = CCSet(),
    _startup_time: Long = System.currentTimeMillis(),
    _try_cnode_idx: Int = 0,
    _net_bcuid: String,
    _pub_key: String = null,
    _pri_key: String = null,
    protocol: String = "", address: String = "", port: Int = 0 //    
    ) extends Node with OLog {

  var masternode: PNode = pnodes(0);

  override def processMessage(gcmd: String, body: Either[Message,ByteString])(implicit network: Network): Unit = {
    //    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    //    MessageSender.postMessage(gcmd, body, this)
    log.debug("procMessage:@" + node_idx + ",bcuid=" + bcuid + ",gcmd:" + gcmd)
    MessageSender.postMessage(gcmd, body, masternode)
  }

  override def toString(): String = {
    "ClusterNode(" + net_id + "," + startup_time + "," + cnode_idx + "," + sign + ")@" + this.hashCode()
  }

  def uri(): String = pnodes(0).protocol + "://" + address + ":" + port;
  def uris(): Array[String] = {
    pnodes.map { n => n.uri() }
  }

  def name(): String = net_id
  def node_idx(): Int = cnode_idx;
  def bcuid(): String = _net_bcuid;
  def pub_key(): String = _pub_key;
  def pri_key(): String = _pri_key
  def counter(): CCSet = _counter
  def startup_time(): Long = _startup_time
  def sign(): String = _sign
  def try_node_idx(): Int = _try_cnode_idx

  override def changeIdx(idx: Int): Node = ClusterNode(
    net_id, idx, //node info
    sign, pnodes, counter, //
    startup_time, //
    idx,
    bcuid,
    pub_key,
    pri_key,
    protocol, address, port //
    )
}

object ClusterNode {

  val prop: PropHelper = new PropHelper(null);

  def genIdx(newidx: Int = -1): Int = {
    var currentidx: Int = newidx
    if (currentidx == -1) {
      currentidx = (Math.abs(Math.random() * 100000 % prop.get("otrans.node.max_nodes", 256))).asInstanceOf[Int];
    }
    val d = prop.get("otrans.cnode.idx", "" + currentidx);
    val envid = System.getProperty("otrans.cnode.idx", d);
    try {
      Integer.parseInt(NodeHelper.envInEnv(envid));
    } catch {
      case _: Throwable =>
        currentidx
    }
  }
}





