package ethanp.cluster

import akka.actor._
import akka.util.Timeout
import ethanp.cluster.ClusterUtil._

import scala.collection.{SortedSet, immutable, mutable}
import scala.concurrent.duration._

/**
 * Ethan Petuchowski
 * 4/9/15
 *
 * Upon creation, it connects to all other servers in the system
 */
class Server extends BayouMem {

    implicit val timeout = Timeout(5.seconds)


    var isPaused = false
    var hasUpdates = false
    var isPrimary = false
    var isRetiring = false
    var isStabilizing = false

    override var nodeID: NodeID = _
    var serverName: ServerName = _
    var myVV = new MutableVV
    var csn: LCValue = 0

    var masterRef: ActorRef = _

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
        // TODO if the song is not in there should it be ERR_DEP or something?
        if (state contains songName) Song(songName, state(songName))
        else Song(songName, "NOT AVAILABLE")
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
        writeLog.lastOption.map { write: Write ⇒
            write.action match {
                case RetireServer(_) ⇒ return // ignore
                case _               ⇒
            }
        }
        writeLog += makeWrite(action) // append
        antiEntropizeAll()            // sync
    }

    /**
     * Initiates anti-entropy sessions with all `connectedServers`
     * Called after writing to the `writeLog`
     */
    def antiEntropizeAll(): Unit =
        if (!isPaused) {
            broadcastServers(LemmeUpgradeU)
            hasUpdates = false
        }
        else hasUpdates = true

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
        immutable.SortedSet.empty[Write] ++ writeLog filter { write ⇒
            def acceptedSince = vec isOlderThan write.acceptStamp
            def newlyCommitted = write.committed && write.commitStamp > commitNo
            acceptedSince || newlyCommitted
        }
    )

    def createServer(c: CreateServer) = {
        val servers = c.servers filterNot (_._1 == nodeID)
        if (servers.isEmpty) {
            log.info("assuming I'm first server, becoming primary with name '0'")

            // assume role of primary
            isPrimary = true

            // assign my name
            serverName = AcceptStamp(0, null)

            // init my VersionVector
            myVV = new MutableVV(mutable.Map(serverName → 0))

            masterRef ! IExist(nodeID)
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

        /* Append a retirement-write
         * This disables future writes from being appended to the log */
        writeLog += makeWrite(Retirement(serverName))

        if (isPrimary) {
            /* find another primary */
            connectedServers.head._2 ! URPrimary
        }

        /* tell someone else of my retirement */
        connectedServers.head._2 ! LemmeUpgradeU

        isRetiring = true


        // step down
        isPrimary = false

        /**
         * tell my clients of a different server to connect to
         * though if by the time the client connects the NEW server has already retired
         * there are going to be issues. But I don't think they'll test that.
         */
        val newDaddy = connectedServers.head
        clients foreach (_ ! ServerSelection(id=newDaddy._1, sel=newDaddy._2))

        /* Different path strings I tried out before going with just sending the selection object */
//        println(self.path)
//        println(connectedServers.head._2.anchorPath)
//        println(connectedServers.head._2.pathString)
//        println(connectedServers.head._2)
    }

    def updateWrites(w: UpdateWrites): Unit = {

        val writeLogAcceptStamps: Map[ServerName, LCValue] =
            writeLog.toList.map(w ⇒ w.acceptStamp → w.commitStamp).toMap

        // ignore anything I have verbatim, or if I have the committed version already
        val rcvdWrites = w.writes filterNot { w ⇒
            def haveWriteVerbatim = writeLog contains w
            def haveMatchingAcceptStamp = writeLogAcceptStamps contains w.acceptStamp
            def itIsCommitted = writeLogAcceptStamps(w.acceptStamp) < INF
            def haveCommittedVersion = haveMatchingAcceptStamp && itIsCommitted

            haveWriteVerbatim || haveCommittedVersion
        }

        if (rcvdWrites isEmpty) return

        if (isStabilizing) masterRef ! Updating

        if (isPrimary) {
            // stamp and add writes
            writeLog ++= rcvdWrites map (_ commit nextCSN)
        }
        else {
            val commits = rcvdWrites filter (_.committed)

            if (commits.nonEmpty) {
                /* update csn */
                csn = (commits maxBy (_.commitStamp)).commitStamp

                /* remove "tentative" writes that have "committed" */
                val newTimestamps = (commits map (_.acceptStamp)).toSet
                writeLog = writeLog filterNot (w ⇒ w.tentative && (newTimestamps contains w.acceptStamp))
            }

            // insert all the new writes
            writeLog ++= rcvdWrites
        }

        myVV updateWith rcvdWrites

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
         *  2. my current writeLog
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
        case Start ⇒
            isPaused = false
            antiEntropizeAll()

        case Stabilize ⇒
            isStabilizing = true
            antiEntropizeAll()

        case DoneStabilizing ⇒ isStabilizing = false

        case Hello ⇒
            println(s"server $nodeID present and connected to ${connectedServers.keys.toList}")
            println(s"server $nodeID log is ${writeLog.toList}")
            println(s"server $nodeID VV is $myVV")
            println(s"server $nodeID csn is $csn")

        case CreationWrite ⇒ creationWrite()

        case URPrimary ⇒ isPrimary = true

        case GangInitiation(name, log, commNum, vv) ⇒
            serverName = name
            writeLog   = log
            csn        = commNum
            myVV       = MutableVV(vv)
            masterRef ! IExist(nodeID)

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
         * then exit the macro-cluster.
         * My nodeID will never be used again.
         */
        case m: RetireServer ⇒ retireServer(m)

        /**
         * Print my complete `writeLog` to `StdOut` in the specified format
         */
        case PrintLog(id) ⇒ writeLog flatMap (_.strOpt) foreach println

        /**
         * The Master has assigned me a logical id
         * which is used hereafter on the command line to refer to me.
         *
         * While we're at it, save a reference to the master
         */
        case IDMsg(id) ⇒
            masterRef = sender()
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
         */
        case ClientConnected ⇒
            clients += sender
            masterRef ! Gotten // master unblocks on CreateClient

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
                if (isRetiring) {

                    /* calls `handleNext` on master waiting on retirement to complete */
                    masterRef ! Gotten

                    /*
                     * `context stop self` --- current msg will be the last processed before exiting
                     * `self ! PoisonPill` --- current mailbox will be processed, then actor will exit
                     */
                    self ! PoisonPill
                }

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
