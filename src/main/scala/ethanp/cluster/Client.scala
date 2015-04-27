package ethanp.cluster

import akka.actor._
import ethanp.cluster.ClusterUtil.NodeID

/**
 * Ethan Petuchowski
 * 4/9/15
 */
class Client extends BayouMem {

    // TODO we're assuming client can only connect to a SINGLE server, right?
    // hopefully will be answered some day: https://piazza.com/class/i5h1h4rqk9t4si?cid=97
    var server: ActorSelection = _
    var serverID: NodeID = _
    var masterRef: ActorRef = _
    override var nodeID: NodeID = -4 // "unset"

    override def handleMsg: PartialFunction[Any, Unit] = {

        case IDMsg(id)  ⇒
            masterRef = sender()
            nodeID = id

        case m: Get ⇒ server ! m

        case m: PutAndDelete ⇒
            server ! m
            masterRef ! Gotten

        case m: IDMsg ⇒ server ! m // instead of `forward` bc I want this `Client` to be the `sender`

        /** reply from Server for Get request */
        case s @ Song(name, url) ⇒
            println(s.str)
            masterRef ! Gotten

        /**
         * This is sent by the master on memberUp(clientMember)
         */
        case ServerPath(id, path) =>
            serverID = id
            server = ClusterUtil getSelection path
            server ! ClientConnected(nodeID)

        /** Current server is retiring, and this is the info for a new one */
        case ServerSelection(id, sel) ⇒
            serverID = id
            server = sel

        /* don't reply with `ClientConnected` because that will
           make the server send an extra Gotten to Master */
//            server ! ClientConnected

        case Hello ⇒ println(s"client $nodeID connected to server $serverID")

        case KillEmAll ⇒ context.system.shutdown()

        case m ⇒ log error s"client received non-client command: $m"
    }
}

object Client {
    def main(args: Array[String]): Unit = ClusterUtil joinClusterAs "client"
}
