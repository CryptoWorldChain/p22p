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

@NActorProvider
object MessageSender extends NActor with OLog {

  //http. socket . or.  mq  are ok
  @PSender
  @BeanProperty
  var sockSender: IPacketSender = null;

  def appendUid(pack: BCPacket, node: PNode): Unit = {
    if (node != NodeInstance.root()
      && !StringUtils.equals(node.bcuid, NodeInstance.root().bcuid)) {
      pack.putHeader(PackHeader.PACK_TO, node.bcuid);
      pack.putHeader(PackHeader.PACK_URI, node.uri);
    } else {
      log.debug("No need appendUid for local node");
    }
    pack.putHeader(PackHeader.PACK_FROM, NodeInstance.root().bcuid);
  }
  def sendMessage(gcmd: String, body: Message, node: PNode, cb: CallBack[FramePacket]) {
    val pack = BCPacket.buildSyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    appendUid(pack, node)
    log.debug("sendMessage:" + pack)
    sockSender.asyncSend(pack, cb)
  }

  def wallMessage(gcmd: String, body: MessageOrBuilder, directBcuid: String = null) {
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    if (directBcuid != null) {
      pack.putHeader(PackHeader.PACK_TO, directBcuid);
      sockSender.post(pack)
    }

    Networks.instance.directNodes.map { node =>
      if (directBcuid == null || !StringUtils.equals(directBcuid, node.bcuid)) {
        appendUid(pack, node)
        sockSender.post(pack)
      }
    }

    log.debug("wallMessage.OK:" + pack)
  }

  def postMessage(gcmd: String, body: MessageOrBuilder, node: PNode) {
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    appendUid(pack, node)
    log.debug("postMessage:" + pack)
    sockSender.post(pack)
  }

  def postMessage(gcmd: String, body: MessageOrBuilder, bcuid: String) {
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    pack.putHeader(PackHeader.PACK_TO, bcuid);
    log.debug("postMessage:bcuid:" + pack)
    sockSender.post(pack)
  }

  def replyPostMessage(frompack: FramePacket, body: MessageOrBuilder) {
    val gcmd = frompack.getModuleAndCMD;
    val pack = BCPacket.buildAsyncFrom(body, gcmd.substring(0, 3), gcmd.substring(3));
    pack.putHeader(PackHeader.PACK_TO, frompack.getExtStrProp(PackHeader.PACK_FROM));
    log.debug("reply_postMessage:bcuid:" + pack)
    sockSender.post(pack)
  }

  def replyWallMessage(frompack: FramePacket, body: MessageOrBuilder) {
    val gcmd = frompack.getModuleAndCMD;
    log.debug("replyWallMessage:" + body)
    wallMessage(gcmd, body, frompack.getExtStrProp(PackHeader.PACK_FROM));
    
  }

  def dropNode(node: PNode) {
    sockSender.tryDropConnection(node.bcuid);
  }

  def changeNodeName(oldName: String, newName: String) {
    sockSender.changeNodeName(oldName, newName);
  }
}

