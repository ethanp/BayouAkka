package ethanp.cluster.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestActorRef}
import ethanp.cluster.{LemmeUpgradeU, IDMsg, Server}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Ethan Petuchowski
 * 4/13/15
 */
class ServerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ClusterSystem"))
  override def afterAll { TestKit shutdownActorSystem system }

  // here we obtain an actual "Server" object
  val serverRef = TestActorRef[Server]
  val serverPtr = serverRef.underlyingActor

  // now we can do normal unit testing with the `actor`
  // but also, messages sent to the `actorRef` are processed *synchronously*
  //      on the current thread and answers may be sent back as usual.

  /* TODO recall that I'm having an issue with the breakConnection
  specifically, the breakConnection 0 2 is only taking effect on 2 and not 0 when I run (quickly)

      joinServer 0
      joinServer 1
      joinServer 2
      breakConnection 0 2
      joinClient 3 0
      joinClient 4 2
      hello

      The problem is probably that 0 doesn't know about 2 by the time it gets this message.

      Possible ways to fix it:

        - delay the `handleNext` call by the clusterKing
        - have node 0 "remember" that it has to disconnect from 2 if 2 if tries to connect to it
   */

  /* TODO I think I could get the from -> to print to work
    if you look at what happens on an "IExist() reception in the server,
    this is what I have to look-up in the map!
   */

  /*  TODO this can simulate EXACTLY the scenario in which my problem arises!

    What remains is to figure out what the problem actually is...

    Here is the system's STATE when it stops working:

      server 0 present and connected to List(1)
      server 1 present and connected to List(0, 2)
      server 2 present and connected to List(1)
      client 3 present!
      client 4 present!
      server 0 log is List(Write(1,Timestamp(1,ServerName(0)),CreationWrite), Write(2,Timestamp(2,ServerName(0)),CreationWrite))
      server 1 log is List(Write(1,Timestamp(1,ServerName(0)),CreationWrite))
      server 2 log is List(Write(1,Timestamp(1,ServerName(0)),CreationWrite), Write(2,Timestamp(2,ServerName(0)),CreationWrite))
      server 0 VV is Map(ServerName(Write(1,Timestamp(1,ServerName(0)),CreationWrite)) -> 1, ServerName(Write(2,Timestamp(2,ServerName(0)),CreationWrite)) -> 2, ServerName(0) -> 2)
      server 1 VV is Map(ServerName(Write(1,Timestamp(1,ServerName(0)),CreationWrite)) -> 1, ServerName(0) -> 1)
      server 2 VV is Map(ServerName(Write(1,Timestamp(1,ServerName(0)),CreationWrite)) -> 1, ServerName(Write(2,Timestamp(2,ServerName(0)),CreationWrite)) -> 2, ServerName(0) -> 2)

    Then what SHOULD happen is:

    node 0 rcvd: Put(3,a,123)

    this gets added to the state, so the log becomes

      same += Write(3,Timestamp(3,ServerName(0)),Put)

    and the VV becomes

      same except ServerName(0) -> 3


   */

  "a server actor" must {
    "set its id when told" in {
      serverRef ! IDMsg(0)
      assert(serverPtr.nodeID == 0)
    }
    "respond with its VV to an Anti-Entropy request" in {
      serverRef ! LemmeUpgradeU
    }
  }

}
