package org.fc.brewchain.p22p

import scala.beans.BeanProperty

import com.google.protobuf.Message

import lombok.extern.slf4j.Slf4j
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.SessionModules
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.ojpa.api.DomainDaoSupport
import onight.tfw.ojpa.api.annotations.StoreDAO
import onight.tfw.oparam.api.OParam
import onight.tfw.otransio.api.IPacketSender
import onight.tfw.otransio.api.PSender
import onight.tfw.ntrans.api.annotation.ActorRequire
import org.fc.brewchain.p22p.pbgens.P22P.PModule
import org.brewchain.bcapi.backend.ODBSupport
import org.brewchain.bcapi.backend.ODBDao
import onight.tfw.ojpa.api.StoreServiceProvider
import onight.tfw.ntrans.api.ActorService
import org.fc.brewchain.bcapi.EncAPI

abstract class PSMPZP[T <: Message] extends SessionModules[T] with PBUtils with OLog {
  override def getModule: String = PModule.PZP.name()
}

@NActorProvider
@Slf4j
object Daos extends PSMPZP[Message] with ActorService {

  @StoreDAO(target = "bc_bdb", daoClass = classOf[ODSP22p])
  @BeanProperty
  var odb: ODBSupport = null

  @StoreDAO(target = "bc_bdb", daoClass = classOf[ODSViewStateStorage])
  @BeanProperty
  var viewstateDB: ODBSupport = null

  @ActorRequire(name = "bdb_provider", scope = "global")
  @BeanProperty
  var bdbprovider: StoreServiceProvider = null;

  @ActorRequire(name = "bc_encoder",scope = "global")
  @BeanProperty
  var enc: EncAPI = null;

  def setOdb(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      odb = daodb.asInstanceOf[ODBSupport];
    } else {
      log.warn("cannot set odb ODBSupport from:" + daodb);
    }
  }
  def setViewstateDB(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      viewstateDB = daodb.asInstanceOf[ODBSupport];
    } else {
      log.warn("cannot set viewstateDB ODBSupport from:" + daodb);
    }
  }
  def isDbReady(): Boolean = {
    return odb != null && odb.getDaosupport.isInstanceOf[ODBSupport] &&
      viewstateDB != null && viewstateDB.getDaosupport.isInstanceOf[ODBSupport] &&
      bdbprovider != null && enc!=null;
  }

  @BeanProperty
  @PSender
  var pSender: IPacketSender = null;

  @BeanProperty
  @ActorRequire(name = "http", scope = "global")
  var httpsender: IPacketSender = null;

}


