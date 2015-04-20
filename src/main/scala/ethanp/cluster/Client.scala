package ethanp.cluster

import akka.actor._
import ethanp.cluster.ClusterUtil.NodeID

/**
 * Ethan Petuchowski
 * 4/9/15
 */
class Client extends BayouMem {

    // TODO we're assuming client can only connect to a SINGLE server, right?
    var server: ActorSelection = _
    override var nodeID: NodeID = _

    override def handleMsg: PartialFunction[Any, Unit] = {

        case IDMsg(id)  ⇒ nodeID = id
        case m: Forward ⇒ server ! m // instead of `forward` bc I want this `Client` to be the `sender`
        case s @ Song(name, url) ⇒ println(s.str)

        case ServerPath(path) =>
            server = ClusterUtil getSelection path
            server ! ClientConnected

        case Hello ⇒ println(s"client $nodeID present!")
        case m ⇒ log error s"client received non-client command: $m"
    }
}

object Client {
    def main(args: Array[String]): Unit = ClusterUtil joinClusterAs "client"
}
