package ethanp.cluster

import akka.actor._
import ethanp.cluster.ClusterUtil.{ServerName, LCValue, NodeID}

/**
 * Ethan Petuchowski
 * 4/9/15
 */
class Client extends BayouMem {

    /* ≤ 1 server connected at a time */
    var server: ActorSelection = _
    var serverID: NodeID = _
    var masterRef: ActorRef = _
    var serverName: ServerName = _
    override var nodeID: NodeID = -4 // "unset"

    /**
     * "To find an acceptable server, the session manager
     *  must check that one or both of these session vectors
     *  are dominated by the server’s version vector."
     */
    val readVec = new MutableVV
    val writeVec = new MutableVV

    var currWID: LCValue = 0
    def nextWID(): LCValue = { currWID += 1; currWID }

    override def handleMsg: PartialFunction[Any, Unit] = {

        case IDMsg(id)  ⇒
            masterRef = sender()
            nodeID = id

        /** Client's "Read" command */
        case m: Get ⇒ server ! m

        /**
         * These are the Client "Write" commands
         *
         *  - To provide the Session Guarantees, we must compare the server's VV with ours
         *    in different ways depending on what we're trying to do.
         *  - To compare, we could either request theirs or send ours.
         *  - I think it would be 1 less step to just send ours with the request, then
         *    the Server can decide to send back ERR_DEP or whatever is appropriate.
         *  - Clients do NOT cache data they read.
         *  - I am assuming each test-script corresponds to a SINGLE "session"
         */
        case m: PutAndDelete ⇒
            // TODO change reception of this from Put & Delete on Server-side
            server ! ClientWrite(ImmutableVV(writeVec), m)
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
            // server will respond with its ServerName (see below)

        case m: ServerName ⇒
            serverName = m
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
