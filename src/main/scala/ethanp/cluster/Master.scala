package ethanp.cluster

import java.lang.System.err
import java.util.Scanner

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus.Up
import akka.cluster.{Cluster, Member}
import ethanp.cluster.Common._

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
    def createClientProc() = s"sbt execClient".run()
    def createServerProc() = s"sbt execServer".run()

    /**
     * Create a new Client Actor (inside this process) that will join the macro-cluster.
     * It won't know it's console-ID yet, we'll tell it that once it joins the cluster.
     */
    def createClient(cid: NodeID, sid: NodeID) = {
        clientID = cid
        serverID = sid
        Client.main(Array.empty)
    }

    /**
     * Create a new Server Actor (inside this process) that will join the macro-cluster.
     * It won't know it's console-ID yet, we'll tell it that once it joins the cluster.
     */
    def createServer(sid: NodeID) = {
        serverID = sid
        Server.main(Array.empty)
    }

    /**
     * Make the Master Actor the first seed node in the Cluster, i.e. the one standing by
     * waiting for new nodes to ask to join the cluster so that it can say a resounding YES!
     */
    val clusterKing = Common.joinClusterAs("2551", "master")

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
     */
    new Thread {
        val sc = new Scanner(System.in)
        while (sc hasNextLine) {
            handle(sc nextLine)
        }
    }

    /**
     * Sends command-line commands to the Master Actor as messages.
     * Would theoretically work even if the CLI and Master were on different continents.
     */
    def handle(str: String): Unit = {

        // TODO I MUST make this blocking, perhaps by having it return a Future or a Promise

        val brkStr = str split " "
        println(s"handling { $str }")
        lazy val b1 = brkStr(1) // 'lazy' because element does not exist unless it is needed
        lazy val b2 = brkStr(2)
        lazy val b1i = b1.toInt
        lazy val b2i = b2.toInt
        brkStr head match {
            case "joinServer"           ⇒ createServer(b1i)
            case "joinClient"           ⇒ createClient(b1i, b2i)
            case "retireServer"         ⇒ clusterKing ! RetireServer(id = b1i)
            case "breakConnection"      ⇒ clusterKing ! BreakConnection(id1 = b1i, id2 = b2i)
            case "restoreConnection"    ⇒ clusterKing ! RestoreConnection(id1 = b1i, id2 = b2i)
            case "pause"                ⇒ clusterKing ! Pause
            case "start"                ⇒ clusterKing ! Start
            case "stabilize"            ⇒ clusterKing ! Stabilize
            case "printLog"             ⇒ clusterKing ! PrintLog(id = b1i)
            case "get"                  ⇒ clusterKing ! Get(clientID = b1i, songName = b2)
            case "delete"               ⇒ clusterKing ! Delete(clientID = b1i, songName = b2)
            case "put"                  ⇒ clusterKing ! Put(clientID = b1i, songName = b2, url = brkStr(3))
        }
    }
}

/**
 * Receives command line commands from the CLI and routes them to the appropriate receivers
 *
 * TODO somehow it has to inform the CLI when this is DONE so it can stop blocking.
 */
class Master extends Actor with ActorLogging {

    /** O.G: "get the Cluster owning the ActorSystem that this actor belongs to"
     * E.P: ...by contacting the "seed nodes" spec'd in the config (repeatedly until one responds).
     *   I think this means this nodes entire ActorSystem is going to
     *     become a part of the Cluster OF Actor Systems!
     */
    val cluster = Cluster(context.system)

    /**
     * Sign me up to be notified when a member joins or is removed from the macro-cluster
     */
    override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp], classOf[MemberRemoved])
    override def postStop(): Unit = cluster unsubscribe self

    var members = Map.empty[NodeID, Member]

    def servers: Map[NodeID, Member] = members collect { case (k, v) if v.roles.head == "server" ⇒ k → v }
    def clients: Map[NodeID, Member] = members collect { case (k, v) if v.roles.head == "client" ⇒ k → v }

    def refFromMember(m: Member): ActorSelection = getSelection(getPath(m), context)
    def getMember(id: NodeID):    ActorSelection = refFromMember(members(id))

    def serverPaths: Map[NodeID, ActorPath] = servers map { case (i, mem) ⇒ i → getPath(mem) }

    // cooking up some curry, namean
    def broadcastServers(msg: Msg): Msg ⇒ Unit = broadcast(servers values)
    def broadcastClients(msg: Msg): Msg ⇒ Unit = broadcast(clients values)

    def broadcast(who: Iterable[Member])(msg: Msg): Unit =
        members foreach { case (i,m) ⇒ refFromMember(m) ! msg }

    override def receive : Actor.Receive = {

        // always sent as 1st msg upon joining the cluster
        case state: CurrentClusterState => state.members filter (_.status == Up) foreach identity

        /* CLI Events */
        case m @ Forward(id) ⇒ getMember(id) forward m
        case m @ Forward2(i,j) ⇒ Seq(i,j) foreach (getMember(_) forward m)
        case m: BrdcstServers ⇒ broadcastServers(m)

        /* Cluster Events */
        case MemberUp(m) =>
              m.roles.head match {
            case "client" ⇒
                val cid: NodeID = Master.clientID
                val sid: NodeID = Master.serverID

                if (cid == -1) { err println "cid hasn't been set"; return null }
                if (sid == -1) { err println "sid hasn't been set"; return null }
                if (members contains cid) { err println s"Node $cid already exists"; return null }
                if (!(members contains sid)) { err println s"Node $sid doesn't exist"; return null }

                members += (cid → m)  // save reference to this member

                // tell the client the server it is supposed to connect to
                val client = refFromMember(m)
                client ! ServerPath(getPath(members(Master.serverID)))

            case "server" ⇒
                val sid: NodeID = Master.serverID

                if (sid == -1) { err println "sid hasn't been set"; return null }
                if (members contains sid) { err println s"Node $sid already exists"; return null }

                members += (sid → m)  // save reference to this member

                val server = refFromMember(m)
                server ! IDMsg(sid)
                server ! CreateServer(serverPaths)

            case "master" ⇒ log info "ignoring Master MemberUp"
        }
//
        /* member has been removed from the cluster
         * time it takes to go from "unreachable" to "down" (and therefore removed)
         * is configured by e.g. "auto-down-unreachable-after = 1s"     */
        case MemberRemoved(m, prevStatus) ⇒ members = members filterNot { case (k, v) ⇒ m == v }
    }
}
