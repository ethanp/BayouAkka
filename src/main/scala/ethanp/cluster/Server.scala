package ethanp.cluster

import akka.actor._

/**
 * Ethan Petuchowski
 * 4/9/15
 */
class Server extends Actor with ActorLogging {

    // I'm supposing these are the clients for whom this
    // server is responsible for informing of updates
    var clients = Set.empty[ActorRef]
    var nodeID: Int = _
    var connectedServers = Set.empty[ActorSelection]

    def receive = {
        case ClientConnected ⇒ clients += sender
        case NodeID(id) ⇒ nodeID = id
    }
}

object Server {
    def main(args: Array[String]): Unit =
        (Common joinClusterAs "server").actorOf(Props[Server], name = "server")
}
