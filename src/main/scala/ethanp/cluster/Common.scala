package ethanp.cluster

import akka.actor._
import akka.cluster.Member
import com.typesafe.config.ConfigFactory

/**
 * Ethan Petuchowski
 * 4/10/15
 */
object Common {

    type NodeID = Int
    type LCValue = Int
    type URL = String

    val INF: LCValue = Integer.MAX_VALUE

    def joinClusterAs(role: String): ActorRef = Common.joinClusterAs("0", role)
    def joinClusterAs(port: String, role: String) = {
        val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
          withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [$role]")).
          withFallback(ConfigFactory.load())
        val s = ActorSystem("ClusterSystem", config)
        role match {
            case "server" ⇒ s.actorOf(Props[Server], name = "server")
            case "client" ⇒ s.actorOf(Props[Client], name = "client")
            case "master" ⇒ s.actorOf(Props[Master], name = "master")
        }
    }
    /**
     * Gets the (Protocol, IP-Address, Port) information for a node in the cluster.
     */
    def getPath(m: Member) = RootActorPath(m.address) / "user" / m.roles.head

    /**
     * Gets a reference to an object that will create socket to a node in the cluster
     * on demand when a message needs to be sent. (I think.)
     */
    def getSelection(path: ActorPath, context: ActorContext) = context actorSelection path
}
