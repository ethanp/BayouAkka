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

    var isPrimary: Boolean = false

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

    var myVersionVector: VersionVector = _

    def nextTimestamp = {
        logicalClock += 1
        Timestamp(logicalClock, serverName)
    }

    def appendAndSync(action: Action) = {
        writeLog += Write(Common.INF, nextTimestamp, action)    // append
        antiEntropizeAll()                                      // sync
    }

    /**
     * Initiates anti-entropy sessions with all `connectedServers`
     * Called after writing to the `writeLog`
     */
    def antiEntropizeAll(): Unit = connectedServers.values foreach (_ ! LemmeUpgradeU)

    def deal(w: Forward2, f: NodeID ⇒ Unit): Unit = if (w.i == nodeID) f(w.j) else f(w.i)

    def findUpdatesGiven(versionVector: VersionVector): UpdateWrites = {
        ???
    }

    def updateLog(writes: Seq[Write]): Unit = {
        ???
    }

    def receive = {
        case m: Forward ⇒ m match {

            case RetireServer(id) ⇒
                if (isPrimary) {
                    // TODO find another primary
                }

            case PrintLog(id) ⇒ writeLog.foreach(w ⇒ println(w.str))
            case IDMsg(id) ⇒ nodeID = id

            case p @ Put(clientID, songName, url) => appendAndSync(p)
            case d @ Delete(clientID, songName) => appendAndSync(d)

            // TODO should return ERR_DEP when necessary (not sure when that IS yet)
            case Get(clientID, songName) => sender ! getSong(songName)
            case _ => log error s"server wasn't expecting msg $m"
        }

        case m: Forward2 ⇒ m match {
            case fwd @ BreakConnection(id1, id2) ⇒ deal(fwd, connectedServers -= _)
            case fwd @ RestoreConnection(id1, id2) ⇒ deal(fwd, id ⇒ connectedServers += id → knownServers(id))
        }
        case Servers(servers: Map[NodeID, ActorPath]) ⇒ addServers(servers)
        case ClientConnected ⇒ clients += sender

        // theoretically, this should happen in a child-actor
        //  -- "each actor should only do one thing"
        case m: AntiEntropyMsg ⇒ m match {
            case LemmeUpgradeU ⇒ sender ! myVersionVector
            case vv: VersionVector ⇒ sender ! findUpdatesGiven(vv)
            case UpdateWrites(writes) ⇒ updateLog(writes)
        }
    }

    def addServers(servers: Map[NodeID, ActorPath]) {
        if (servers.isEmpty) {
            // become "Primary Server"
            log.info("becoming primary server")
            isPrimary = true
        }

        connectedServers ++= servers map { case (id, path) ⇒
            id → getSelection(path, context)
        }
        knownServers ++= connectedServers
    }
}

object Server {
    def main(args: Array[String]): Unit =
        (Common joinClusterAs "server").actorOf(Props[Server], name = "server")
}
