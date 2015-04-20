package ethanp.cluster

import akka.actor._
import akka.util.Timeout
import ethanp.cluster.ClusterUtil._

import scala.collection.{SortedSet, mutable}
import scala.concurrent.duration._

/**
 * Ethan Petuchowski
 * 4/9/15
 *
 * Upon creation, it connects to all other servers in the system
 */
class Server extends BayouMem {

    implicit val timeout = Timeout(5.seconds)

    // TODO still need to impmt join protocol where this is assigned
    var myVV = new MutableVV

    var isPaused: Boolean = false
    var isPrimary: Boolean = false
    override var nodeID: NodeID = _
    var serverName: ServerName = _
    var csn: LCValue = 0

    var clients = Set.empty[ActorRef]
    var writeLog = SortedSet.empty[Write]

    var connectedServers = Map.empty[NodeID, ActorSelection]
    var knownServers = Map.empty[NodeID, ActorSelection]

    def logicalClock: LCValue = myVV(serverName)
    def incrementMyLC(): LCValue = myVV increment serverName

    /**
     * For now instead of having a persistent state and an undo log etc, we just play through....``
     * TODO the easiest next-step would be to store all stable writes and only replay tentatives
     * The final step of course is to implement the undo-log, though they don't specify that
     * any of this is req'd in the spec....
     */
    def getSong(songName: String): Song = {
        val state = mutable.Map.empty[String, URL]
        for (w ← writeLog) {
            w.action match {
                case Put(_, name, url) => state(name) = url
                case Delete(_, name)   => state remove name
                case _ ⇒ // ignore
            }
        }
        // TODO if the song is not in there it crashes. Maybe it should be ERR_DEP or something?
        Song(songName, state(songName))
    }

    def makeWrite(action: Action) =
        if (isPrimary) Write(nextCSN, nextTimestamp, action)
        else Write(INF, nextTimestamp, action)

    def nextCSN = {
        csn += 1
        csn
    }

    def nextTimestamp = AcceptStamp(incrementMyLC(), serverName)

    def appendAndSync(action: Action): Unit = {
        writeLog += makeWrite(action) // append
        antiEntropizeAll()            // sync
    }

    /**
     * Initiates anti-entropy sessions with all `connectedServers`
     * Called after writing to the `writeLog`
     */
    def antiEntropizeAll(): Unit = if (!isPaused) broadcastServers(LemmeUpgradeU)

    def broadcastServers(msg: Msg): Unit = connectedServers.values foreach (_ ! msg)

    /**
     * Apply `f` to the node specified in the `Forward2` who is not me
     */
    def otherID[T](w: Forward2, f: NodeID ⇒ T): T = if (w.i == nodeID) f(w.j) else f(w.i)

    /**
     * From (Lec 11, pg. 6)
     * If "S" (me) finds out that R_i (`vec`s owner) doesn't know of R_j = (TS_{k,j},R_k)
     *    - If `vec(R_k) ≥ TS_{k,j}`, don't forward writes ACCEPTED by R_j
     *    - Else send R_i all writes accepted by R_j
     *
     * @return all updates strictly after the given `versionVector` and commits since CSN
     */
    def allWritesSince(vec: ImmutableVV, commitNo: LCValue) = UpdateWrites(
            writeLog filter { write ⇒
                def acceptedSince = vec before write.acceptStamp
                def newlyCommitted = write.committed && write.commitStamp > commitNo
                acceptedSince || newlyCommitted
            })

    def createServer(c: CreateServer) = {
        val servers = c.servers filterNot (_._1 == nodeID)
        if (servers.isEmpty) {
            log.info("assuming I'm first server, becoming primary with name '0'")

            // assume role of primary
            isPrimary = true

            // assign my name
            // TODO could be implicit conversion String -> ServerName
            serverName = AcceptStamp(0, null)

            // init my VersionVector
            myVV = new MutableVV(mutable.Map(serverName → 0))
        }
        else {
            // save existing servers
            connectedServers ++= servers map { case (id, path) ⇒
                id → getSelection(path)
            }
            knownServers ++= connectedServers

            // tell them that I exist
            broadcastServers(IExist(nodeID))

            /**
             * "A Bayou server S_i creates itself by sending a 'creation write' to another server S_k
             *  Any server for the database can be used."
             */
            connectedServers.head._2 ! CreationWrite
        }
    }

    def retireServer(server: RetireServer): Unit = {
        if (isPrimary) {
            // TODO find another primary
        }
        // append a retirement-write
        writeLog += makeWrite(Retirement(serverName))

        // TODO tell someone else of my retirement
        //                connectedServers

        /**
         * context stop self --- the current msg will be the last processed before exiting
         * self ! PoisonPill --- first process current mailbox, then actor will exit
         */
        context stop self
    }

    def updateWrites(w: UpdateWrites): Unit = {
        val newWrites = w.writes

        if (newWrites isEmpty) return

        if (isPrimary) {

            // stamp and add writes
            writeLog ++= newWrites map (_ commit nextCSN)
        }
        else {
            val commits = newWrites filter (_.committed)

            if (commits.nonEmpty) {
                // update csn
                csn = (commits maxBy (_.commitStamp)).commitStamp

                /* remove "tentative" writes that have "committed" */
                val newTimestamps = (commits map (_.acceptStamp)).toSet
                writeLog = writeLog filterNot (w ⇒ w.tentative && (newTimestamps contains w.acceptStamp))
            }

            // insert all the new writes
            writeLog ++= newWrites
        }

        myVV updateWith newWrites

        Thread sleep 300 // TODO remove

        // propagate them out ("gossip")
        antiEntropizeAll()
    }

    def creationWrite(): Unit = {

        // get timestamp
        val write = makeWrite(CreationWrite)

        // name them
        val serverName = write.acceptStamp

        // add creation to writeLog
        writeLog += write

        // add them to the version vector
        myVV addNewMember (serverName → logicalClock)

        /**
         * Tell Them:
         *  1. their new name
         *  2. my current writeLog (TODO kind-of a cheap-shot, don't'ya think?)
         *  3. my current CSN
         *  4. my VV
          */
        sender ! GangInitiation(serverName, writeLog, csn, ImmutableVV(myVV))
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

        case Pause ⇒ isPaused = true
        case Start ⇒ isPaused = false
        case Stabilize ⇒ isPaused = false // TODO not sure how this is going to work

        case Hello ⇒
            println(s"server $nodeID present and connected to ${connectedServers.keys.toList}")
            println(s"server $nodeID log is ${writeLog.toList}")
            println(s"server $nodeID VV is $myVV")

        case CreationWrite ⇒ creationWrite()

        case GangInitiation(name, log, commNum, vv) ⇒
            serverName  = name
            writeLog    = log
            csn         = commNum
            myVV        = MutableVV(vv)

        case s: ServerName ⇒

            // "<T_{k,i}, S_k> becomes S_i’s server-id."
            serverName = s


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
        case Get(clientID, songName) =>
            println(s"getting song $songName")
            val song: Song = getSong(songName)
            println(s"found song $song")
            sender ! song

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
            case LemmeUpgradeU ⇒ sender ! CurrentKnowledge(ImmutableVV(myVV), csn)

            /**
             * Bayou Haiku:
             * ------------
             * Tell me what you've heard,
             * I will tell you what I know,
             * where I know you don't.
             */
            case CurrentKnowledge(vec, commitNo) ⇒
                sender ! allWritesSince(vec, commitNo)

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
