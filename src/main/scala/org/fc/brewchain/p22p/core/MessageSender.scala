package org.fc.brewchain.p22p.core

import onight.tfw.otransio.api.PSender
import onight.tfw.otransio.api.IPacketSender
import scala.beans.BeanProperty
import onight.osgi.annotation.NActorProvider
import com.google.protobuf.Message
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.async.CallBack
import onight.tfw.ntrans.api.NActor
import onight.oapi.scala.traits.OLog
import org.fc.brewchain.p22p.node.PNode
import onight.tfw.otransio.api.PackHeader
import org.fc.brewchain.bcapi.BCPacket
import org.fc.brewchain.p22p.node.NodeInstance
import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.p22p.node.Networks
import com.google.protobuf.MessageOrBuilder

import org.brewchain.bcapi.utils.PacketIMHelper._
import scala.collection.TraversableLike

@NActorProvider
object MessageSender extends NActor with OLog {

  //http. socket . or.  mq  are ok
  @PSender
  @BeanProperty
  var sockSender: IPacketSender = null;

  def appendUid(pack: BCPacket, node: PNode): Unit = {
    if (NodeInstance.isLocal(node)) {
      pack.getExtHead.remove(PackHeader.PACK_TO);
    } else {
      pack.putHeader(PackHeader.PACK_TO, node.bcuid);
      pack.putHeader(PackHeader.PACK_URI, node.uri);
    }
    pack.putHeader(PackHeader.PACK_FROM, NodeInstance.root().bcuid);
  }
  def appendUid(pack: BCPacket, bcuid: String): Unit = {
    if (NodeInstance.isLocal(bcuid)) {
      pack.getExtHead.remove(PackHeader.PACK_TO);
    } else {
      pack.putHeader(PackHeader.PACK_TO, bcuid);
    }
    pack.putHeader(PackHeader.PACK_FROM, NodeInstance.root().bcuid);
  }
  def sendMessage(gcmd: String, body: Message, node: PNode, cb: CallBack[FramePacket]) {
    val pack = BCPacket.buildSyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    appendUid(pack, node)
    log.debug("sendMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    sockSender.asyncSend(pack, cb)
  }

  def wallMessageToPending(gcmd: String, body: Message) {
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    log.debug("wallMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    Networks.instance.pendingNodes.map { node =>
      appendUid(pack, node)
      sockSender.post(pack)
    }

    //    log.debug("wallMessage.OK:" + pack.getModuleAndCMD)
  }

  def postMessage(gcmd: String, body: Message, node: PNode) {
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    appendUid(pack, node)
    log.trace("postMessage:" + pack)
    sockSender.post(pack)
  }

  def postMessage(gcmd: String, body: Message, bcuid: String) {
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    appendUid(pack, bcuid);
    log.debug("postMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    sockSender.post(pack)
  }

  def replyPostMessage(frompack: FramePacket, body: Message) {
    val gcmd = frompack.getModuleAndCMD;
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    appendUid(pack, frompack.getExtStrProp(PackHeader.PACK_FROM));
    log.trace("reply_postMessage:" + pack.getModuleAndCMD + ",F=" + pack.getFrom() + ",T=" + pack.getTo())
    sockSender.post(pack)
  }

  def replyWallMessage(frompack: FramePacket, body: Message) {
    val gcmd = frompack.getModuleAndCMD;
    //    Networks.instance.pendingNodes
    log.trace("replyWallMessage:" + body)
    wallMessageToPending(gcmd, body);
  }

  def dropNode(node: PNode) {
    sockSender.tryDropConnection(node.bcuid);
  }

  def changeNodeName(oldName: String, newName: String) {
    sockSender.changeNodeName(oldName, newName);
  }
}

