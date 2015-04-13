package ethanp.cluster

import akka.actor.ActorPath

/**
 * Ethan Petuchowski
 * 4/9/15
 */
sealed class Msg()
sealed trait MasterMsg extends Msg
class Forward(val i: Int) extends MasterMsg
object Forward { def unapply(fwd: Forward): Option[Int] = Some(fwd.i) }
trait BrdcstServers extends MasterMsg
case class  RetireServer(id: Int)                   extends Forward(id)
case class  BreakConnection(id1: Int, id2: Int)     extends Forward(id1)
case class  RestoreConnection(id1: Int, id2: Int)   extends Forward(id1)
case class  PrintLog(id: Int)                       extends Forward(id)
case class  NodeID(id: Int)                         extends Forward(id) with Administrativa
case class  Put(clientID: Int, songName: String, url: String) extends Forward(clientID)
case class  Get(clientID: Int, songName: String)    extends Forward(clientID)
case class  Delete(clientID: Int, songName: String) extends Forward(clientID)
case object Pause                                   extends BrdcstServers
case object Start                                   extends BrdcstServers
case object Stabilize                               extends BrdcstServers

sealed trait Administrativa extends Msg
case class  ServerPath(path: ActorPath)             extends Administrativa
case object ClientConnected                         extends Administrativa
