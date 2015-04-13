package ethanp.cluster

import akka.actor.ActorPath

/**
 * Ethan Petuchowski
 * 4/9/15
 */
sealed trait ClientCommand
sealed trait ServerCommand
sealed case class MasterMsg()
case class  RetireServer(id: Int)                   extends MasterMsg with ServerCommand
case class  BreakConnection(id1: Int, id2: Int)     extends MasterMsg with ServerCommand
case class  RestoreConnection(id1: Int, id2: Int)   extends MasterMsg with ServerCommand
case object Pause                                   extends MasterMsg with ServerCommand
case object Start                                   extends MasterMsg with ServerCommand
case object Stabilize                               extends MasterMsg with ServerCommand
case class  PrintLog(id: Int)                       extends MasterMsg with ServerCommand
case class  Put(clientID: Int, songName: String, url: String) extends MasterMsg with ClientCommand
case class  Get(clientID: Int, songName: String)    extends MasterMsg with ClientCommand
case class  Delete(clientID: Int, songName: String) extends MasterMsg with ClientCommand

sealed case class Administrativa()
case class  NodeID(id: Int)                         extends Administrativa with ServerCommand
case class  ServerPath(path: ActorPath)             extends Administrativa with ClientCommand
case object ClientConnected                         extends Administrativa with ServerCommand
