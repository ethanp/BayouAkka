package ethanp.cluster

import akka.actor._
import akka.cluster.Member
import com.typesafe.config.ConfigFactory
import ethanp.cluster.ClusterUtil.NodeID

/**
 * Ethan Petuchowski
 * 4/10/15
 */
object ClusterUtil {

    type NodeID = Int
    type LCValue = Int
    type URL = String

    val INF: LCValue = Integer.MAX_VALUE

    def joinClusterAs(role: String): ActorRef = ClusterUtil.joinClusterAs("0", role)

    def joinClusterAs(port: String, role: String): ActorRef = {
        val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
                withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [$role]")).
                withFallback(ConfigFactory.load())
        val system = ActorSystem("ClusterSystem", config)
        role match {
            case "server" ⇒ system.actorOf(Props[Server], name = "server")
            case "client" ⇒ system.actorOf(Props[Client], name = "client")
            case "master" ⇒ system.actorOf(Props[Master], name = "master")
        }
    }

    /**
     * Gets the (Protocol, IP-Address, Port) information for a node in the cluster.
     */
    def getPath(m: Member): ActorPath = RootActorPath(m.address) / "user" / m.roles.head

    /**
     * Gets a reference to an object that will create socket to a node in the cluster
     * on demand when a message needs to be sent. (I think.)
     */
    def getSelection(path: ActorPath)(implicit context: ActorContext): ActorSelection = context actorSelection path
}

trait BayouMem extends Actor with ActorLogging {

    var nodeID: NodeID

    val printMsg: PartialFunction[Any, Msg] = {
        case any: Msg ⇒
            println(s"node $nodeID rcvd $any")
            any
    }

    def handleMsg: PartialFunction[Msg, Unit]

    val printReceive: PartialFunction[Any, Unit] = printMsg andThen handleMsg
    override def receive: PartialFunction[Any, Unit] = printReceive
}
