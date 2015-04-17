package ethanp.cluster

import akka.actor.ActorPath
import ethanp.cluster.ClusterUtil.{INF, LCValue, NodeID, URL}

import scala.collection.SortedSet
import scala.collection.mutable
import scala.collection.immutable

/**
 * Ethan Petuchowski
 * 4/9/15
 */
sealed class Msg() extends Serializable
sealed trait MasterMsg extends Msg
sealed trait Action {
    def str: Option[String]
}

class Forward(val i: NodeID) extends MasterMsg
class Forward2(val i: NodeID, val j: NodeID) extends MasterMsg
object Forward { def unapply(fwd: Forward): Option[NodeID] = Some(fwd.i) }
object Forward2 { def unapply(fwd: Forward2): Option[(NodeID, NodeID)] = Some(fwd.i, fwd.j) }

trait BrdcstServers extends MasterMsg
case class  RetireServer(id: NodeID)                    extends Forward(id)
case class  BreakConnection(id1: NodeID, id2: NodeID)   extends Forward2(id1, id2)
case class  RestoreConnection(id1: NodeID, id2: NodeID) extends Forward2(id1, id2)
case class  PrintLog(id: NodeID)                        extends Forward(id)
case class  IDMsg(id: NodeID)                           extends Forward(id) with Administrativa

case class  Put(clientID: NodeID, songName: String, url: String) extends Forward(clientID) with Action {
    override def str: Option[String] = Some(s"PUT:($songName, $url):")
}
case class Delete(clientID: NodeID, songName: String) extends Forward(clientID) with Action {
    override def str: Option[String] = Some(s"PUT:($songName):")
}
case class Get(clientID: NodeID, songName: String) extends Forward(clientID)
case class Song(songName: String, url: URL) extends Msg {
    def str: String = s"$songName:$url"
}
case class Retirement(serverName: ServerName) extends Action {
    override def str: Option[String] = None
}

case object Pause     extends BrdcstServers
case object Start     extends BrdcstServers
case object Stabilize extends BrdcstServers

sealed trait Administrativa extends Msg
case class  ServerPath(path: ActorPath) extends Administrativa
case class  CreateServer(servers: Map[NodeID, ActorPath]) extends Administrativa
case object ClientConnected             extends Administrativa
case class  IExist(nodeID: NodeID)      extends Administrativa

case class Write(acceptStamp: LCValue, timestamp: Timestamp, action: Action) extends Ordered[Write] {
    override def compare(that: Write): Int = timestamp compare that.timestamp

    /* 'OP_TYPE:(OP_VALUE):STABLE_BOOL' */
    def strOpt = action.str map { _ + { if (acceptStamp == INF) "TRUE" else "FALSE" } }
    def commit(stamp: LCValue) = Write(stamp, timestamp, action)
}
object Write {
    def apply(action: Action) = Write
}
case class Timestamp(lcVal: LCValue, acceptor: ServerName) extends Ordered[Timestamp] {
    override def compare(that: Timestamp): Int =
        if (lcVal != that.lcVal) {
            lcVal compare that.lcVal
        }
        else {
            acceptor compare that.acceptor
        }
}
case class ServerName(name: String) extends Msg with Ordered[ServerName] {
    // I think any (associative, commutative, reflexive, transitive) comparison is probably fine
    override def compare(that: ServerName): Int = name compare that.name
}

sealed trait AntiEntropyMsg extends Msg
case object LemmeUpgradeU extends AntiEntropyMsg
case class UpdateWrites(writes: SortedSet[Write]) extends AntiEntropyMsg
case class CurrentKnowledge(versionVector: ImmutableVV, csn: LCValue) extends AntiEntropyMsg
case object Hello extends Msg
case class NewClient(cid: NodeID, sid: NodeID)  extends Msg
case class NewServer(sid: NodeID) extends Msg
case object CreationWrite extends Msg with Action {
    override def str: Option[String] = None // TODO is it correct to not print CreationWrites ?
}

/**
 * For ease of use within servers, I'd like to have a _mutable_ version vector,
 * But for doing message passing, I don't want to pass a mutable object bc I've heard that's
 *  "bad" and I don't know enough about it to be able to evaluate it for this case, so I'm
 *  just going to stay away from doing that, so I have an _immutable_ version that can be
 *  created out of the mutable one and vice-versa.
 */
sealed trait VersionVector extends Ordered[VersionVector] {
    override def compare(that: VersionVector): Int = ??? // I know this one, just haven't needed it
    def get(serverName: ServerName): LCValue = vectorMap(serverName)
    val vectorMap: scala.collection.Map[ServerName, LCValue]
    def knowsAbout(name: ServerName) = vectorMap contains name
    def isNotSince(ts: Timestamp): Boolean = {
        def newerAcceptorThanIKnow = ??? // TODO
        if (knowsAbout(ts.acceptor))
            ts.lcVal > vectorMap(ts.acceptor)
        else newerAcceptorThanIKnow
    }
    def apply(name: ServerName): LCValue = vectorMap(name)
}
case class ImmutableVV(vectorMap: immutable.Map[ServerName, LCValue] = immutable.Map.empty) extends VersionVector {

}
class MutableVV(val vectorMap: mutable.Map[ServerName, LCValue] = mutable.Map.empty) extends VersionVector {
    def increment(name: ServerName): LCValue = {
        vectorMap(name) += 1
        vectorMap(name)
    }
    def addMember(name: ServerName) = {
        ??? // TODO
    }
}

object ImmutableVV {
    def apply(mut: MutableVV): ImmutableVV = ImmutableVV(mut.vectorMap.toMap)
}

object MutableVV {
    def apply(imm: ImmutableVV): MutableVV = new MutableVV(mutable.Map() ++ imm.vectorMap)
}
