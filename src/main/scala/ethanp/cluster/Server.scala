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

    def receive = {
        case ClientConnected ⇒ clients += sender
    }
}

object Server {
    def main(args: Array[String]) =
        Common.clusterSystem("server").actorOf(Props[Server], name = "server")
}
