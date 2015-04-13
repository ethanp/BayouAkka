package ethanp.cluster

import akka.actor._
import ethanp.cluster.Common.{LCValue, NodeID, getSelection}

/**
 * Ethan Petuchowski
 * 4/9/15
 *
 * Upon creation, it connects to all other servers in the system
 */
class Server extends Actor with ActorLogging {

    var nodeID: NodeID = _
    var serverName: ServerName = _

    // I'm supposing these are the clients for whom this
    // server is responsible for informing of updates
    var clients = Set.empty[ActorRef]

    var connectedServers = Map.empty[ActorSelection, NodeID]
    var writeLog = collection.mutable.SortedSet.empty[Write]

    var logicalClock: LCValue = 0

    def receive = {
        case m: Forward ⇒ m match {
            case RetireServer(id) =>
            case BreakConnection(id1, id2) =>
            case RestoreConnection(id1, id2) =>
            case PrintLog(id) =>
            case IDMsg(id) => nodeID = id

            case p @ Put(clientID, songName, url) =>
                writeLog += Write(Common.INF, Timestamp(logicalClock, serverName), p)

            case Get(clientID, songName) =>
            case Delete(clientID, songName) =>
            case _ =>
        }
        case Servers(servers: Map[ActorPath, NodeID]) ⇒ addServers(servers)

        case ClientConnected ⇒ clients += sender
    }

    def addServers(servers: Map[ActorPath, NodeID]) {
        servers foreach { case (k, v) ⇒
            connectedServers += (getSelection(k, context) → v)
        }
    }
}

object Server {
    def main(args: Array[String]): Unit =
        (Common joinClusterAs "server").actorOf(Props[Server], name = "server")
}
