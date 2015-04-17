package ethanp.cluster

import akka.actor.ActorPath
import ethanp.cluster.ClusterUtil.{INF, LCValue, NodeID, URL}

import scala.collection.SortedSet

/**
 * Ethan Petuchowski
 * 4/9/15
 */
sealed class Msg()
sealed trait MasterMsg extends Msg
sealed trait Action {
    def str: Option[String]
}

trait Forward extends MasterMsg {
    val i: NodeID
}
trait Forward2 extends MasterMsg {
    val i: NodeID
    val j: NodeID
}
object Forward { def unapply(fwd: Forward): Option[NodeID] = Some(fwd.i) }
object Forward2 { def unapply(fwd: Forward2): Option[(NodeID, NodeID)] = Some(fwd.i, fwd.j) }

trait BrdcstServers extends MasterMsg
case class  RetireServer(i: NodeID)                 extends Forward
case class  BreakConnection(i: NodeID, j: NodeID)   extends Forward2
case class  RestoreConnection(i: NodeID, j: NodeID) extends Forward2
case class  PrintLog(i: NodeID)                     extends Forward
case class  IDMsg(i: NodeID)                        extends Forward with Administrativa

case class  Put(i: NodeID, songName: String, url: String) extends Forward with Action {
    override def str: Option[String] = Some(s"PUT:($songName, $url):")
}
case class Delete(i: NodeID, songName: String) extends Forward with Action {
    override def str: Option[String] = Some(s"PUT:($songName):")
}
case class Get(i: NodeID, songName: String) extends Forward
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
    def str = action.str map { _ + { if (acceptStamp == INF) "TRUE" else "FALSE" } }
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
case class ServerName(name: String) extends Ordered[ServerName] {
    // I think any (associative, commutative, reflexive, transitive) comparison is probably fine
    override def compare(that: ServerName): Int = name compare that.name
}

sealed trait AntiEntropyMsg extends Msg
case object LemmeUpgradeU extends AntiEntropyMsg
case class VersionVector(vectorMap: Map[ServerName, LCValue] = Map.empty[ServerName, LCValue])
        extends Ordered[VersionVector] {
    def knowsAbout(name: ServerName) = vectorMap contains name
    override def compare(that: VersionVector): Int = ??? // I know this one, just haven't needed it

    def isNotSince(ts: Timestamp): Boolean = {
        def newerAcceptorThanIKnow = ??? // TODO
        if (knowsAbout(ts.acceptor))
            ts.lcVal > vectorMap(ts.acceptor)
        else newerAcceptorThanIKnow

    }
}
case class UpdateWrites(writes: SortedSet[Write]) extends AntiEntropyMsg
case class CurrentKnowledge(versionVector: VersionVector, csn: LCValue) extends AntiEntropyMsg
case object Hello extends Msg
case class NewClient(cid: NodeID, sid: NodeID)
case class NewServer(sid: NodeID)
