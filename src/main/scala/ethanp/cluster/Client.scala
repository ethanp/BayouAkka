package ethanp.cluster

import akka.actor._
import ethanp.cluster.ClusterUtil.NodeID

/**
 * Ethan Petuchowski
 * 4/9/15
 */
class Client extends Actor with ActorLogging {

    // TODO we're assuming client can only connect to a SINGLE server, right?
    var server: ActorSelection = _
    var myID:   NodeID = _

    override def receive: PartialFunction[Any, Unit] = {

        case IDMsg(id) ⇒ myID = id
        case Hello ⇒ println(s"client $myID present!")
        case m: Forward ⇒ server forward m


        case ServerPath(path) =>
            server = ClusterUtil.getSelection(path)
            server ! ClientConnected

        case s@Song(name, url) ⇒ println(s.str)

        case m ⇒ log error s"client received non-client command: $m"
    }
}

object Client {
    def main(args: Array[String]): Unit = ClusterUtil joinClusterAs "client"
}
