package ethanp.cluster

import akka.actor.ActorPath
import ethanp.cluster.Common.{URL, INF, LCValue, NodeID}

import scala.collection.mutable

/**
 * Ethan Petuchowski
 * 4/9/15
 */
sealed class Msg()
sealed trait MasterMsg extends Msg
sealed trait Action {
    def str: String
}
class Forward(val i: NodeID) extends MasterMsg
object Forward { def unapply(fwd: Forward): Option[NodeID] = Some(fwd.i) }
trait BrdcstServers extends MasterMsg
case class  RetireServer(id: NodeID)                    extends Forward(id)
case class  BreakConnection(id1: NodeID, id2: NodeID)   extends Forward(id1)
case class  RestoreConnection(id1: NodeID, id2: NodeID) extends Forward(id1)
case class  PrintLog(id: NodeID)                        extends Forward(id)
case class  IDMsg(id: NodeID)                           extends Forward(id) with Administrativa

case class  Put(clientID: NodeID, songName: String, url: String) extends Forward(clientID) with Action {
    override def str: String = s"PUT:($songName, $url):"
}
case class Delete(clientID: NodeID, songName: String) extends Forward(clientID) with Action {
    override def str: String = s"PUT:($songName):"
}
case class Get(clientID: NodeID, songName: String) extends Forward(clientID) {
    def str(url: URL): String = s"$songName:$url"
}

case object Pause                                       extends BrdcstServers
case object Start                                       extends BrdcstServers
case object Stabilize                                   extends BrdcstServers

sealed trait Administrativa extends Msg
case class  ServerPath(path: ActorPath)                 extends Administrativa
case class  Servers(servers: Map[ActorPath, NodeID])    extends Administrativa
case object ClientConnected                             extends Administrativa

case class Write(acceptStamp: LCValue, timestamp: Timestamp, action: Action) extends Ordered[Write] {
    override def compare(that: Write): NodeID = timestamp compare that.timestamp

    /* 'OP_TYPE:(OP_VALUE):STABLE_BOOL' */
    def str = action.str + { if (acceptStamp == INF) "TRUE" else "FALSE" }
}
case class Timestamp(lcVal: LCValue, acceptor: ServerName) extends Ordered[Timestamp] {
    override def compare(that: Timestamp): NodeID =
        if (lcVal != that.lcVal) lcVal compare that.lcVal
        else acceptor compare that.acceptor
}
case class VersionVector(m: mutable.Map[ServerName, LCValue]) extends Ordered[VersionVector] {
    override def compare(that: VersionVector): NodeID = ???
}
case class ServerName(name: String) extends Ordered[ServerName] {
    // I think any (associative, commutative, reflexive, transitive) comparison is probably fine
    override def compare(that: ServerName): NodeID = name compare that.name
}
