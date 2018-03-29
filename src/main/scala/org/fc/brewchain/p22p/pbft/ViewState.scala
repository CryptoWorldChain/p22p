package org.fc.brewchain.p22p.pbft

import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.p22p.Daos
import onight.oapi.scala.traits.OLog
import java.util.concurrent.atomic.AtomicInteger
import lombok.extern.slf4j.Slf4j
import onight.osgi.annotation.NActorProvider

case class ViewState(N: Int = 0, V: Int = 0, from: String = null, sign: String = "") extends OLog {

}
