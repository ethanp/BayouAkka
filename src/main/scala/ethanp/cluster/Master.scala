package ethanp.cluster

import java.lang.System.err
import java.util.Scanner

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus.Up
import akka.cluster.{Cluster, Member}
import ethanp.cluster.Common.{getPath, getSelection}

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

    def createClientProc() = s"sbt execClient".run()
    def createServerProc() = s"sbt execServer".run()
    def createClient() = Client.main(Array.empty)
    def createServer() = Server.main(Array.empty)

    /* make the master the first seed node */
    val clusterKing = Common.clusterSystem("2551", "master").actorOf(Props[Master], name = "master")

    @volatile var clientID = -1
    @volatile var serverID = -1

    /* THE COMMAND LINE INTERFACE */
    val sc = new Scanner(System.in)
    new Thread {
        while (sc.hasNextLine) {
            val str = sc.nextLine
            val brkStr = str split " "
            println(s"handling { $str }")
            brkStr.head match {
                case "joinServer" ⇒
                    serverID = brkStr(1).toInt
                    createServer()

                case "joinClient" ⇒
                    clientID = brkStr(1).toInt // the client's ID is ONLY relevant to the Master
                    serverID = brkStr(2).toInt
                    createClient()

                case "start" ⇒

                case "sendMessage" ⇒
            }
        }
    }
}

class Master extends Actor with ActorLogging {

    /* O.G: "get the Cluster owning the ActorSystem that this actor belongs to"
     * E.P: ...by contacting the "seed nodes" spec'd in the config (repeatedly until one responds).
     *   I think this means this nodes entire ActorSystem is going to
     *     become a part of the Cluster OF Actor Systems!
     */
    val cluster = Cluster(context.system)

    override def preStart(): Unit = cluster.subscribe(self,
                 classOf[MemberUp], classOf[MemberRemoved])

    override def postStop(): Unit = cluster.unsubscribe(self)

    var members = Map.empty[Int, Member]

    def firstFreeID(set: Set[Int]) = (Stream from 0 filterNot (set contains)) head

    override def receive = {

        // Note: this may not work properly anymore but I don't think it matters
        case state: CurrentClusterState => state.members filter (_.status == Up) foreach newMember

        case MemberUp(m) => newMember(m)

        /* member has been removed from the cluster
         * time it takes to go from "unreachable" to "down" (and therefore removed)
         * is configured by e.g. "auto-down-unreachable-after = 1s"     */
        case MemberRemoved(m, prevStatus) ⇒ remove(m)
    }

    def newMember(m: Member): Unit = {
        m.roles.head match {
            case "client" ⇒
                val cid: Int = Master.clientID
                val sid: Int = Master.serverID

                if (cid == -1) { err println "cid hasn't been set"; return }
                if (sid == -1) { err println "sid hasn't been set"; return }
                if (members contains cid) { err println s"Node $cid already exists"; return }
                if (!(members contains sid)) { err println s"Node $sid doesn't exist"; return }

                members += (cid → m)  // save reference to this member

                // tell the client the server it is supposed to connect to
                getSelection(getPath(m), context) ! ServerPath(getPath(members(Master.serverID)))

            case "server" ⇒
                val sid: Int = Master.serverID

                if (sid == -1) { err println "sid hasn't been set"; return }
                if (members contains sid) { err println s"Node $sid already exists"; return }

                members += (sid → m)  // save reference to this member

            case "master" ⇒ log.info("ignoring Master MemberUp")
        }
    }

    def remove(m: Member) { members = members filterNot { case (k, v) ⇒ m == v } }
}
