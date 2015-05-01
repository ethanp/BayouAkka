package ethanp.cluster

import akka.actor._
import ethanp.cluster.ClusterUtil.NodeID

/**
 * Ethan Petuchowski
 * 4/9/15
 */
class Client extends BayouMem {

    /* ≤ 1 server connected at a time */
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

        /** reply from Server for Get request */
        case s @ Song(name, url) ⇒
            println(s.str)
            masterRef ! Gotten

        /**
         * This is sent by the master on memberUp(clientMember)
         * and on RestoreConnection
         */
        case ServerPath(id, path) =>
            serverID = id
            server = ClusterUtil getSelection path
            server ! ClientConnected(nodeID)
            masterRef ! Gotten // master unblocks on CreateClient & RestoreConnection

        /** Current server is retiring, and this is the info for a new one */
        case ServerSelection(id, sel) ⇒
            serverID = id
            server = sel

        /* don't reply with `ClientConnected` because that will
           make the server send an extra Gotten to Master */
//            server ! ClientConnected

        case BreakConnection(a, b) ⇒
            def oneIsMe = a == nodeID || b == nodeID
            def oneIsMyServer = a == serverID || b == serverID
            if (oneIsMe && oneIsMyServer) server = null
            else log error s"client $nodeID rcvd break-conn for non-existent server"
            if (a == nodeID) masterRef ! Gotten

        case Hello ⇒ println(s"client $nodeID connected to server $serverID")

        case KillEmAll ⇒ context.system.shutdown()

        case m ⇒ log error s"client received non-client command: $m"
    }
}

object Client {
    def main(args: Array[String]): Unit = ClusterUtil joinClusterAs "client"
}
