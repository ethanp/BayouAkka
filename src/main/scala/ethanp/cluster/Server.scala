package ethanp.cluster

import akka.actor._
import akka.util.Timeout
import ethanp.cluster.ClusterUtil._

import scala.collection.SortedSet
import scala.concurrent.duration._

/**
 * Ethan Petuchowski
 * 4/9/15
 *
 * Upon creation, it connects to all other servers in the system
 */
class Server extends BayouMem {

    implicit val timeout = Timeout(5.seconds)
    var logicalClock: LCValue = 0

    // TODO still need join protocol where this is assigned
    var myVersionVector: VersionVector = _

    var isPrimary: Boolean = false
    override var nodeID: NodeID = _
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

    def appendAndSync(action: Action): Unit = {
        writeLog += makeWrite(action) // append
        antiEntropizeAll()            // sync
    }

    /**
     * Initiates anti-entropy sessions with all `connectedServers`
     * Called after writing to the `writeLog`
     */
    def antiEntropizeAll(): Unit = broadcastServers(LemmeUpgradeU)

    def broadcastServers(msg: Msg): Unit = connectedServers.values foreach (_ ! msg)

    /**
     * Apply `f` to the node specified in the `Forward2` who is not me
     */
    def otherID[T](w: Forward2, f: NodeID ⇒ T): T = if (w.i == nodeID) f(w.j) else f(w.i)

    /**
     * @return all updates strictly after the given `versionVector` and commits since CSN
     */
    def allWritesSince(vec: VersionVector, commitNo: LCValue): UpdateWrites =
        UpdateWrites(writeLog filter (w ⇒ (vec isNotSince w.timestamp) || w.acceptStamp > commitNo))



    def retireServer(server: RetireServer): Unit = {
        if (isPrimary) {
            // TODO find another primary
        }
        // append a retirement-write
        writeLog += makeWrite(Retirement(serverName))

        // TODO tell someone else of my retirement
        //                connectedServers

        // TODO wait for them to "ACK" it or something? Use an `?` for that?

        /**
         * context stop self --- the current msg will be the last processed before exiting
         * self ! PoisonPill --- first process current mailbox, then actor will exit
         */
        context stop self
    }

    def createServer(c: CreateServer) = {
        val servers = c.servers filterNot (_._1 == nodeID)
        if (servers.isEmpty) {
            log.info("assuming I'm first server, becoming primary with name '0'")

            // assume role of primary
            isPrimary = true

            // assign my name
            // TODO could be implicit conversion String -> ServerName
            serverName = ServerName("0")

            // init my VersionVector
            myVersionVector = VersionVector(Map(serverName → logicalClock))
        }
        else {
            // save existing servers
            connectedServers ++= servers map { case (id, path) ⇒
                id → getSelection(path)
            }
            knownServers ++= connectedServers

            // tell them that I exist
            broadcastServers(IExist(nodeID))

            // `ask` (the first) one for a name; blocking till it is received
            // of course it needn't be the first one, could be e.g. the one with the shortest name
            connectedServers.head._2 ! CreationWrite
        }
    }

    def updateWrites(w: UpdateWrites): Unit = {
        val newWrites = w.writes
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

    override def handleMsg: PartialFunction[Msg, Unit] = {

        /**
         * "Breaking the connection" here means I never send them anything.
         * Different nodes don't generally maintain longstanding TCP connections,
         * they only create a socket connection when they are actively messaging each other;
         * so there is no connection to physically break.
         * They all remain connected to the macro-cluster at all times.
         */
        case fwd: BreakConnection   ⇒ otherID(fwd, connectedServers -= _)
        case fwd: RestoreConnection ⇒ otherID(fwd, id ⇒ connectedServers += id → knownServers(id))

        case Hello ⇒ println(s"server $nodeID present!")

        case CreationWrite ⇒
            // add creation to writeLog
            val write = makeWrite(CreationWrite)
            writeLog += write

            // reply with their new name
            sender ! ServerName(write.toString)


        case s: ServerName ⇒
            serverName = s

            // TODO retrieve someone's complete write log?

            // TODO create version vector

            // TODO assign logical clock value

        /**
         * Received by a server who was just added to the 'macro-cluster',
         * uses this info to fully become one of the gang.
         */
        case m: CreateServer ⇒ createServer(m)

        /**
         * Received from the Master.
         * Instructs me to go through the retirement procedure,
         * then exit the macro-cluster
         */
        case m: RetireServer ⇒ retireServer(m)

        /**
         * Print my complete `writeLog` to `StdOut` in the specified format
         */
        case PrintLog(id) ⇒ writeLog flatMap (_.strOpt) foreach println

        /**
         * The Master has assigned me a logical id
         * which is used hereafter on the command line to refer to me
         */
        case IDMsg(id) ⇒
            nodeID = id
            log info s"server id set to $nodeID"

        /**
         * Client has submitted new log entry that I should replicate
         */
        case p: Put    ⇒ appendAndSync(p)
        case d: Delete ⇒ appendAndSync(d)

        /**
         * Send the client back the Song they requested
         * TODO should return ERR_DEP when necessary (not sure when that IS yet)
         */
        case Get(clientID, songName) => sender ! getSong(songName)



        /**
         * Received by all connected servers from a new server in the macro-cluster
         * who wants to receive epidemics just like all the cool kids.
         */
        case IExist(id) ⇒
            val serverPair = id → getSelection(sender.path)
            connectedServers += serverPair
            knownServers += serverPair

        /**
         * Sent by a client for whom this server is the only one it knows (I think).
         *
         *  Q: What about if this guy crashes?
         *  A: Nodes cannot simply 'crash'
         *
         *  TODO I need to tell the client when I'm "retiring"
         */
        case ClientConnected ⇒ clients += sender

        /**
         * Theoretically this could be taking place in a spawned child-actor (solo si tendría tiempo).
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
            case m: UpdateWrites ⇒ updateWrites(m)
        }
    }
}


object Server {
    def main(args: Array[String]): Unit = ClusterUtil joinClusterAs "server"
}
