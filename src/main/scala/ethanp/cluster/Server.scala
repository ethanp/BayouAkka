package ethanp.cluster

import akka.actor._
import ethanp.cluster.Common._

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

    var connectedServers = Map.empty[NodeID, ActorSelection]
    var knownServers = Map.empty[NodeID, ActorSelection]
    var writeLog = collection.mutable.SortedSet.empty[Write]
    var databaseState = Map.empty[String, URL]
    def getSong(songName: String) = Song(songName, databaseState(songName))

    var logicalClock: LCValue = 0

    def nextTimestamp = {
        logicalClock += 1
        Timestamp(logicalClock, serverName)
    }

    def appendAndSync(action: Action) = {
        // append
        writeLog += Write(Common.INF, nextTimestamp, action)

        // sync
        antiEntropizeAll
    }

    /**
     * Initiates anti-entropy sessions with all `connectedServers`
     * Called after writing to the `writeLog`
     */
    def antiEntropizeAll = {
        for (server ← connectedServers.values) {
            antiEntropizeWith(server)
        }
    }

    def receive = {
        case m: Forward ⇒ m match {

            case RetireServer(id) ⇒


            case BreakConnection(id1, id2) ⇒ connectedServers -= id2

            case RestoreConnection(id1, id2) ⇒ connectedServers += (id2 → knownServers(id2))

            case PrintLog(id) ⇒ writeLog.foreach(w ⇒ println(w.str))
            case IDMsg(id) ⇒ nodeID = id

            case p @ Put(clientID, songName, url) => appendAndSync(p)
            case d @ Delete(clientID, songName) => appendAndSync(d)

            // TODO should return ERR_DEP when necessary (not sure when that IS yet)
            case Get(clientID, songName) => sender ! getSong(songName)
            case _ => log error s"server wasn't expecting msg $m"
        }
        case Servers(servers: Map[NodeID, ActorPath]) ⇒ addServers(servers)
        case ClientConnected ⇒ clients += sender
    }

    def addServers(servers: Map[NodeID, ActorPath]) {
        servers foreach { case (id, path) ⇒
            connectedServers += (id → getSelection(path, context))
        }
        knownServers ++= connectedServers
    }
}

object Server {
    def main(args: Array[String]): Unit =
        (Common joinClusterAs "server").actorOf(Props[Server], name = "server")
}
