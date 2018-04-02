package org.fc.brewchain.p22p.utils

import org.apache.commons.lang3.StringUtils
import org.slf4j.MDC
import onight.oapi.scala.traits.OLog
import org.fc.brewchain.p22p.node.NodeInstance

trait LogHelper extends OLog {
  def getAbr(str: String) = StringUtils.abbreviateMiddle(str, "..", 8);
//  def MDCSetBCUID(bcuid: String) = MDC.put("BCUID", getAbr(bcuid));
  
  def MDCSetBCUID() = MDC.put("BCUID",getAbr(NodeInstance.root().bcuid));
  
  def MDCSetMessageID(msgid: String) = MDC.put("MessageID", msgid);
  def MDCRemoveMessageID() = MDC.remove("MessageID");
  
  
}