package ethanp.cluster

import akka.actor._
import ethanp.cluster.Common.{URL, LCValue, NodeID, getSelection}

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
    var knownServers = Map.empty[ActorSelection, NodeID]
    var writeLog = collection.mutable.SortedSet.empty[Write]
    var databaseState = Map.empty[String, URL]
    def getSong(songName: String) = Song(songName, databaseState(songName))

    var logicalClock: LCValue = 0

    def nextTimestamp = {
        logicalClock += 1
        Timestamp(logicalClock, serverName)
    }

    def addToWriteLog(action: Action) = {
        writeLog += Write(Common.INF, nextTimestamp, action)
    }

    def antiEntropize = {

    }

    def receive = {
        case m: Forward ⇒ m match {

            case RetireServer(id) ⇒


            case BreakConnection(id1, id2) ⇒
                connectedServers = connectedServers filter { case (k, v) ⇒ v != id2 }

            case RestoreConnection(id1, id2) ⇒
                connectedServers += (knownServers find { case (k, v) ⇒ v == id2 }).get

            case PrintLog(id) ⇒ writeLog.foreach(w ⇒ println(w.str))
            case IDMsg(id) ⇒ nodeID = id

            case p @ Put(clientID, songName, url) => addToWriteLog(p)
            case d @ Delete(clientID, songName) => addToWriteLog(d)

            // TODO should return ERR_DEP when necessary
            case Get(clientID, songName) => sender ! getSong(songName)
            case _ => log error s"server wasn't expecting msg $m"
        }
        case Servers(servers: Map[ActorPath, NodeID]) ⇒ addServers(servers)
        case ClientConnected ⇒ clients += sender
    }

    def addServers(servers: Map[ActorPath, NodeID]) {
        servers foreach { case (k, v) ⇒
            connectedServers += (getSelection(k, context) → v)
        }
        knownServers ++= connectedServers
    }
}

object Server {
    def main(args: Array[String]): Unit =
        (Common joinClusterAs "server").actorOf(Props[Server], name = "server")
}
