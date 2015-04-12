package ethanp.cluster

import akka.actor.{ActorContext, ActorPath, ActorSystem, RootActorPath}
import akka.cluster.Member
import com.typesafe.config.ConfigFactory

/**
 * Ethan Petuchowski
 * 4/10/15
 */
object Common {
    def clusterSystem(role: String): ActorSystem = Common.clusterSystem("0", role)
    def clusterSystem(port: String, role: String) = {
        val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
          withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [$role]")).
          withFallback(ConfigFactory.load())
        ActorSystem("ClusterSystem", config)
    }
    def getPath(m: Member) = RootActorPath(m.address) / "user" / m.roles.head
    def getSelection(path: ActorPath, context: ActorContext) = context actorSelection path
}
