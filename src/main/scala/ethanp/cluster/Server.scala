package ethanp.cluster

import akka.actor._
import ethanp.cluster.Common._

import scala.collection.SortedSet


/**
 * Ethan Petuchowski
 * 4/9/15
 *
 * Upon creation, it connects to all other servers in the system
 */
class Server extends Actor with ActorLogging {

    var logicalClock: LCValue = 0
    var myVersionVector: VersionVector = _ // TODO still need join protocol where this is assigned
    var isPrimary: Boolean = false
    var nodeID: NodeID = _
    var serverName: ServerName = _
    var csn: LCValue = 0

    var clients          = Set.empty[ActorRef]
    var writeLog         = SortedSet.empty[Write]
    var databaseState    = Map.empty[String, URL]
    var connectedServers = Map.empty[NodeID, ActorSelection]
    var knownServers     = Map.empty[NodeID, ActorSelection]

    def getSong(songName: String) = Song(songName, databaseState(songName))
    def makeWrite(action: Action) = Write(INF, nextTimestamp, action)

    def nextCSN = {
        csn += 1
        csn
    }

    def nextTimestamp = {
        logicalClock += 1
        Timestamp(logicalClock, serverName)
    }

    def appendAndSync(action: Action) = {
        writeLog += makeWrite(action)    // append
        antiEntropizeAll()               // sync
    }

    /**
     * Initiates anti-entropy sessions with all `connectedServers`
     * Called after writing to the `writeLog`
     */
    def antiEntropizeAll(): Unit = connectedServers.values foreach (_ ! LemmeUpgradeU)

    /**
     * Apply `f` to the node specified in the `Forward2` who is not me
     */
    def otherID[T](w: Forward2, f: NodeID ⇒ T): T = if (w.i == nodeID) f(w.j) else f(w.i)

    /**
     * @return all updates strictly after the given `versionVector` and commits since CSN
     */
    def allWritesSince(vec: VersionVector, commitNo: LCValue): UpdateWrites =
        UpdateWrites(writeLog filter (w ⇒ (vec isNotSince w.timestamp) || w.acceptStamp > commitNo))

    def receive = {

        // TODO it could be that the sender() is a Server I've never seen before

        case m: Forward ⇒ m match {

            case RetireServer(id) ⇒
                if (isPrimary) {
                    // TODO find another primary
                }
                // append a retirement-write
                writeLog += makeWrite(Retirement(serverName))

                // TODO tell someone else of my retirement
                connectedServers

                // TODO wait for them to "ACK" it or something

            case PrintLog(id) ⇒ writeLog.foreach(w ⇒ println(w.str))
            case IDMsg(id) ⇒ nodeID = id

            case p @ Put(clientID, songName, url) ⇒ appendAndSync(p)
            case d @ Delete(clientID, songName)   ⇒ appendAndSync(d)

            // TODO should return ERR_DEP when necessary (not sure when that IS yet)
            case Get(clientID, songName) => sender ! getSong(songName)
            case _ => log error s"server wasn't expecting msg $m"
        }

        case m: Forward2 ⇒ m match {
            case fwd: BreakConnection   ⇒ otherID(fwd, connectedServers -= _)
            case fwd: RestoreConnection ⇒ otherID(fwd, id ⇒ connectedServers += id → knownServers(id))
        }

        case CreateServer(servers: Map[NodeID, ActorPath]) ⇒
            if (servers.isEmpty) {
                log.info("becoming primary server")
                isPrimary = true
                // TODO assign a name to myself

                // TODO init my VersionVector
            }
            else addServers(servers)

        case ClientConnected ⇒ clients += sender

        // theoretically, this should happen in a child-actor
        //  -- "each actor should only do one thing"
        case m: AntiEntropyMsg ⇒ m match {
            case LemmeUpgradeU                   ⇒ sender ! CurrentKnowledge(myVersionVector, csn)
            case CurrentKnowledge(vec, commitNo) ⇒ sender ! allWritesSince(vec, commitNo)

            case UpdateWrites(newWrites) ⇒
                if (isPrimary) {
                    writeLog ++= (newWrites map (_ commit nextCSN)) // stamp and add writes
                    antiEntropizeAll()                              // propagate them out
                }
                else {
                    val commits = newWrites filter (_.acceptStamp < INF)
                    
                    // update csn
                    csn = (commits maxBy (_.acceptStamp)).acceptStamp

                    // remove "tentative" writes that have "committed"
                    val newTimestamps = (commits map (_.timestamp)).toSet
                    writeLog = writeLog filter (newTimestamps contains _.timestamp)

                    // insert all the new writes
                    writeLog ++= newWrites
                }
        }
    }

    def addServers(servers: Map[NodeID, ActorPath]) {
        connectedServers ++= servers map { case (id, path) ⇒
            id → getSelection(path, context)
        }
        knownServers ++= connectedServers
    }
}

object Server {
    def main(args: Array[String]) = Common joinClusterAs "server"
}
