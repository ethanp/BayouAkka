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
    var myVersionVector: VersionVector = _
    // TODO still need join protocol where this is assigned
    var isPrimary: Boolean = false
    var nodeID: NodeID = _
    var serverName: ServerName = _
    var csn: LCValue = 0

    var clients = Set.empty[ActorRef]
    var writeLog = SortedSet.empty[Write]
    var databaseState = Map.empty[String, URL]
    var connectedServers = Map.empty[NodeID, ActorSelection]
    var knownServers = Map.empty[NodeID, ActorSelection]

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
        writeLog += makeWrite(action) // append
        antiEntropizeAll()            // sync
    }

    /**
     * Initiates anti-entropy sessions with all `connectedServers`
     * Called after writing to the `writeLog`
     */
    def antiEntropizeAll(): Unit = broadcastServers(LemmeUpgradeU)

    def broadcastServers(msg: Msg) = connectedServers.values foreach (_ ! msg)

    /**
     * Apply `f` to the node specified in the `Forward2` who is not me
     */
    def otherID[T](w: Forward2, f: NodeID ⇒ T): T = if (w.i == nodeID) f(w.j) else f(w.i)

    /**
     * @return all updates strictly after the given `versionVector` and commits since CSN
     */
    def allWritesSince(vec: VersionVector, commitNo: LCValue): UpdateWrites =
        UpdateWrites(writeLog filter (w ⇒ (vec isNotSince w.timestamp) || w.acceptStamp > commitNo))

    //noinspection EmptyParenMethodAccessedAsParameterless
    def receive = {

        case m: Forward ⇒ m match {

            /**
             * Received from the Master.
             * Instructs me to go through the retirement procedure,
             * then exit the macro-cluster
             */
            case RetireServer(id) ⇒
                if (isPrimary) {
                    // TODO find another primary
                }
                // append a retirement-write
                writeLog += makeWrite(Retirement(serverName))

                // TODO tell someone else of my retirement
                connectedServers

                // TODO wait for them to "ACK" it or something

                // TODO System.exit the macro-cluster?

            /**
             * Print my complete `writeLog` to `StdOut` in the specified format
             */
            case PrintLog(id) ⇒ writeLog flatMap (_.str) foreach println

            /**
             * The Master has assigned me a logical id
             * which is used hereafter on the command line to refer to me
             */
            case IDMsg(id) ⇒ nodeID = id

            /**
             * Client has submitted new log entry that I should replicate
             */
            case p@Put(clientID, songName, url) ⇒ appendAndSync(p)
            case d@Delete(clientID, songName) ⇒ appendAndSync(d)

            /**
             * Send the client back the Song they requested
             * TODO should return ERR_DEP when necessary (not sure when that IS yet)
             */
            case Get(clientID, songName) => sender ! getSong(songName)

            case _ => log error s"server wasn't expecting msg $m"
        }

        case m: Forward2 ⇒ m match {

            /**
             * "Breaking the connection" here means I never send them anything.
             * Different nodes don't generally maintain longstanding TCP connections,
             * they only create a socket connection when they are actively messaging each other;
             * so there is no connection to physically break.
             * They all remain connected to the macro-cluster at all times.
             */
            case fwd: BreakConnection   ⇒ otherID(fwd, connectedServers -= _)
            case fwd: RestoreConnection ⇒ otherID(fwd, id ⇒ connectedServers += id → knownServers(id))
        }

        /**
         * Received by a server who was just added to the 'macro-cluster',
         * uses this info to fully become one of the gang.
         */
        case CreateServer(servers: Map[NodeID, ActorPath]) ⇒
            if (servers.isEmpty) {
                log.info("becoming primary server")
                isPrimary = true
                // TODO assign a name to myself

                // TODO init my VersionVector
            }
            else {
                // save existing servers
                connectedServers ++= servers map { case (id, path) ⇒
                    id → getSelection(path, context)
                }
                knownServers ++= connectedServers

                // tell them that I exist
                broadcastServers(IExist(nodeID))

                // TODO `ask()` one of them for an ID; blocking till it is received
            }

        /**
         * Received by all connected servers from a new server in the macro-cluster
         * who wants to receive epidemics just like all the cool kids.
         */
        case IExist(id) ⇒
            val serverPair = id → getSelection(sender.path, context)
            connectedServers += serverPair
            knownServers += serverPair

        /**
         * Sent by a client for whom this server is the only one it knows (I think).
         * TODO But wait what about if this guy crashes, whatcha gon' duu?
         */
        case ClientConnected ⇒ clients += sender

        /**
         * Theoretically this should be taking place in a spawned child-actor (solo si tendría tiempo).
         */
        case m: AntiEntropyMsg ⇒ m match {

            /**
             * Proposition from someone else that they want to update all of my knowledges.
             */
            case LemmeUpgradeU ⇒ sender ! CurrentKnowledge(myVersionVector, csn)

            /**
             * Since I know what you know,
             * I can tell you everything I know,
             * where I know you don't know it.
             */
            case CurrentKnowledge(vec, commitNo) ⇒ sender ! allWritesSince(vec, commitNo)

            /**
             * Those writes they thought I didn't know,
             * including things I knew only "tentatively" that have now "committed".
             */
            case UpdateWrites(newWrites) ⇒
                if (isPrimary) {
                    writeLog ++= (newWrites map (_ commit nextCSN)) // stamp and add writes
                    antiEntropizeAll() // propagate them out
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
}


object Server {
    def main(args: Array[String]) = Common joinClusterAs "server"
}
