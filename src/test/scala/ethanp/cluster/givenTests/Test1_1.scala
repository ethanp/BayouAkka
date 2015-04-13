package ethanp.cluster.givenTests

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import ethanp.cluster.Master
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration._

/**
 * Ethan Petuchowski
 * 4/13/15
 */
//noinspection EmptyParenMethodOverridenAsParameterless
class Test1_1(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ClusterSystem"))


  override def afterAll { TestKit.shutdownActorSystem(system) }

  "An master " must {
    "send back messages unchanged" in {
      Master.handle("joinServer 0")
      within (200 milli) {

      }
//      val echo = system.actorOf(Props[Master])
//      echo ! "hello world"
//      expectMsg("hello world")
    }

  }
}
