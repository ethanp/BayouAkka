package ethanp.cluster

import java.lang.System.err

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import ethanp.cluster.ClusterUtil._

import scala.sys.process._

/**
 * Ethan Petuchowski
 * 4/9/15
 *
 * Receives commands from the command line and deals with them appropriately.
 *
 * Also the "first seed node" i.e. the one that all actors attempting
 * to join the cluster contact first.
 */
object Master extends App {

    /**
     * Create a new process that will join the macro-cluster as the given type
     * @return the process reference
     */
    def createClientProc(): Process = s"sbt execClient".run()

    def createServerProc(): Process = s"sbt execServer".run()

    /**
     * Create a new Client Actor (inside this process) that will join the macro-cluster.
     * It won't know it's console-ID yet, we'll tell it that once it joins the cluster.
     */
    def createClient(cid: NodeID, sid: NodeID) {
        clientID = cid
        serverID = sid
        Client.main(Array.empty)
    }

    /**
     * Create a new Server Actor (inside this process) that will join the macro-cluster.
     * It won't know it's console-ID yet, we'll tell it that once it joins the cluster.
     */
    def createServer(sid: NodeID) {
        serverID = sid
        Server.main(Array.empty)
    }

    /**
     * Make the Master Actor the first seed node in the Cluster, i.e. the one standing by
     * waiting for new nodes to ask to join the cluster so that it can say a resounding YES!
     */
    val clusterKing = ClusterUtil.joinClusterAs("2551", "master")

    /**
     * the client ID that will be assigned to the next Client to join the cluster
     */
    var clientID: NodeID = -1

    /**
     * the server ID that will be assigned to the next Server to join the cluster
     */
    var serverID: NodeID = -1

    /**
     * THE COMMAND LINE INTERFACE
     * --- now, with blocking!
     */
    val sc = new java.util.Scanner(System.in)

    /**
     * we only handle one line at a time,
     * otherwise we are in free-space simply waiting for it to complete,
     * in the callback handler, we try to handle the next line
     */
    def handleNext: Unit = {
        println("handling next")
        handle(sc nextLine)
    }

    /**
     * Sends command-line commands to the Master Actor as messages.
     * Would theoretically work even if the CLI and Master were on different continents.
     */
    def handle(str: String): Unit = {

        val brkStr = str split " "
        println(s"handling { $str }")
        lazy val b1 = brkStr(1) // 'lazy' because element does not exist unless it is needed
        lazy val b2 = brkStr(2)
        lazy val b1i = b1.toInt
        lazy val b2i = b2.toInt
        brkStr head match {
            case "joinServer"        ⇒ createServer(b1i)
            case "joinClient"        ⇒ createClient(b1i, b2i)
            case "retireServer"      ⇒ clusterKing ! RetireServer(id = b1i)
            case "breakConnection"   ⇒ clusterKing ! BreakConnection(id1 = b1i, id2 = b2i)
            case "restoreConnection" ⇒ clusterKing ! RestoreConnection(id1 = b1i, id2 = b2i)
            case "pause"             ⇒ clusterKing ! Pause
            case "start"             ⇒ clusterKing ! Start
            case "stabilize"         ⇒ clusterKing ! Stabilize
            case "printLog"          ⇒ clusterKing ! PrintLog(id = b1i)
            case "get"               ⇒ clusterKing ! Get(clientID = b1i, songName = b2)
            case "delete"            ⇒ clusterKing ! Delete(clientID = b1i, songName = b2)
            case "put"               ⇒ clusterKing ! Put(clientID = b1i, songName = b2, url = brkStr(3))
        }
    }
}

/**
 * Receives command line commands from the CLI and routes them to the appropriate receivers
 *
 * TODO somehow it has to inform the CLI when this is DONE so it can stop blocking.
 */
class Master extends Actor with ActorLogging {
    var members = Map.empty[NodeID, Member]

    /** Sign me up to be notified when a member joins or is removed from the macro-cluster */
    val cluster: Cluster = Cluster(context.system)
    override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp], classOf[MemberRemoved])
    override def postStop(): Unit = cluster unsubscribe self

    def servers: Map[NodeID, Member] = members collect { case (k,v) if v.roles.head == "server" ⇒ k → v }
    def clients: Map[NodeID, Member] = members collect { case (k,v) if v.roles.head == "client" ⇒ k → v }
    def selFromMember(m: Member): ActorSelection = getSelection(getPath(m))

    def getMember(id: NodeID): ActorSelection = selFromMember(members(id))
    def serverPaths: Map[NodeID, ActorPath] = servers map { case (i, mem) ⇒ i → getPath(mem) }
    def broadcastServers(msg: Msg): Msg ⇒ Unit = broadcast(servers values)
    def broadcastClients(msg: Msg): Msg ⇒ Unit = broadcast(clients values)
    def broadcast(who: Iterable[Member])(msg: Msg): Unit = members foreach { case (i,m) ⇒ selFromMember(m) ! msg }

    override def receive: Actor.Receive = {

        /* CLI Events */
        case m@Forward(id) ⇒ getMember(id) forward m
        case m@Forward2(i, j) ⇒ Seq(i, j) foreach (getMember(_) forward m)
        case m: BrdcstServers ⇒ broadcastServers(m)

        /**
         * A member has 'officially' been added to the macro-cluster.
         *
         * We should tell it what it's ID for the command line is, as well as
         *   - For Clients: send them the path to the server it should talk to
         *   - For Servers: send them the paths of all other servers so it can tell them its path
         *   - For Masters: ignore.
         */
        case MemberUp(m) ⇒ integrateNewMember(m)

        /**
         * Member has been 'officially' removed from the macro-cluster.
         * The duration we wait before marking an "unreachable" node as "down"
         * (and therefore removed) is configured by e.g. "auto-down-unreachable-after = 1s"
         */
        case MemberRemoved(m, prevStatus) ⇒ removeMember(m)
    }

    def removeMember(m: Member) { members = members filterNot { case (k, v) ⇒ m == v } }

    def integrateNewMember(m: Member) {
        m.roles.head match {
            case "client" ⇒
                val cid: NodeID = Master.clientID
                val sid: NodeID = Master.serverID

                if (cid == -1) { err println "cid hasn't been set"; return }
                if (sid == -1) { err println "sid hasn't been set"; return }
                if (members contains cid) { err println s"Node $cid already exists"; return }
                if (!(members contains sid)) { err println s"Node $sid doesn't exist"; return }

                members += (cid → m) // save reference to this member

                // tell the client the server it is supposed to connect to
                val client = selFromMember(m)
                client ! ServerPath(getPath(members(Master.serverID)))

            case "server" ⇒
                val sid: NodeID = Master.serverID

                if (sid == -1) { err println "sid hasn't been set"; return }
                if (members contains sid) { err println s"Node $sid already exists"; return }

                members += (sid → m) // save reference to this member

                val server = selFromMember(m)
                server ! IDMsg(sid)
                server ! CreateServer(serverPaths)

            case "master" ⇒ log info "ignoring Master MemberUp"
        }
        Thread sleep 100 // wait a 10th of a second for it to get to that actor
        Master.handleNext
    }
}
