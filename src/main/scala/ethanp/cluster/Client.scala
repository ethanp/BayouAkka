package ethanp.cluster

import akka.actor._

/**
 * Ethan Petuchowski
 * 4/9/15
 */
class Client extends Actor {

  // TODO we're assuming client can only connect to a SINGLE server, right?
  var server: ActorSelection = _

  override def receive = {
    case ServerPath(path) ⇒
      server = Common.getSelection(path, context)
      server ! ClientConnected
  }
}

object Client {
  def main(args: Array[String]): Unit =
    Common.clusterSystem("client").actorOf(Props[Client], name = "client")
}
