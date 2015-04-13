package ethanp.cluster

import akka.actor._

/**
 * Ethan Petuchowski
 * 4/9/15
 */
class Client extends Actor with ActorLogging {

  // TODO we're assuming client can only connect to a SINGLE server, right?
  var server: ActorSelection = _

  override def receive = {
    case m : Forward ⇒ server forward m

    case ServerPath(path) =>
      server = Common.getSelection(path, context)
      server ! ClientConnected

    case m ⇒ log.error(s"client received non-client command: $m")
  }
}

object Client {
  def main(args: Array[String]): Unit =
    (Common joinClusterAs "client").actorOf(Props[Client], name = "client")
}
