package ethanp.cluster.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestActorRef}
import ethanp.cluster.Server
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Ethan Petuchowski
 * 4/13/15
 */
class ServerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

    // here we obtain an actual "Server" object
    val actorRef = TestActorRef[Server]
    val actor = actorRef.underlyingActor

    // now we can do normal unit testing with the `actor`
    // but also, messages sent to the `actorRef` are processed *synchronously*
    //      on the current thread and answers may be sent back as usual.

}
