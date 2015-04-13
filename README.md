# TODO List
1. Anti-Entropy session implementation
2. Automated in-IDE test cases for given tests
    * At least 1 of them should assert proper resulting state as well 
3. `Master.handle(String)` should block until it satisfies the spec
    * A `ConditionVariable` could work, but maybe `Future` or `Promise` could
      be used instead

## Testing
1. `expectMsg[T](d: Duration, msg: T): T`
    * The given message object must be received within the specified time; the
      object will be returned.
2. `awaitCond(p: => Boolean, max: Duration, interval: Duration)`
    * Poll the given condition every interval until it returns true or the max
      duration is used up. The interval defaults to 100 ms and the maximum
      defaults to the time remaining in the innermost enclosing within block.
3. Since an integration test doesn't allow direct asserts on the `Actor`'s
   state, the suggested workaround is to write asserts for logged messages.
   They provide a simple technique for accomplishing this.

## First of All
### DO NOT USE MULTIPLE PROCESSES (yet)
**Yes, I said it.** Akka has this thing called Location Transparency, which
means no code needs to change to support the transition to multiple JVMs, only
configuration changes are needed.

However building the *system* stuff around having multiple processes is going
to be a day or two worth of overhead that I can't afford to be both messing
with now, as well as down the line as the code develops. Getting it to work
would be *awesome*, but it should be something that can just be "enabled" after
the writing of the code, and it's not even clear right now whether it would be
worth it.

#### Second of All
It is `nodeID`, not `nodeId` even though the latter is preferred by the IDE.
The former is preferred by yours truly.


# System
It's an `Akka Cluster`. It runs as multiple "Actors" (shared-nothing objects
that run from a thread-pool) and communicate solely via a message-passing
interface in guaranteed FIFO [but not causal] order.

## Messages

### Within System
1. `RetirementWrite`
2. `CreationWrite`
3. `Write`

### Client to Server
1. `Song(name, url)`
2. `Put(song)`
3. `Get(name)`
4. `Delete(name)`

## ClusterKing
Serves as the ever-standing "seed-node" for the cluster. Also serves as the
voice from the CLI (separate object, see below) to the Cluster.

This is who receives notifications from the cluster that there is a new member, and 

### Command Line Input
There is an `object CommandLine` who simply parks on `StdIn`. It is **not an
actor** because I learned The Hard Way that you can't do that, even within a
separate thread in the actor. The reason why is left as an exercise.

## Server
### Fields

#### Write Log

    type CSM = Int
    type LogicalValue = Int
    case class RecursivePID = ???
    
    case class LogEntry(csm: CSM, lc: LogicalValue, pid: RecursivePID)
                extends Ordered[LogEntry] {
        def compareTo(o: LogEntry) = // not sure this is valid Scala
            if (csm != o.csm) csm - o.csm
            else if (lc != o.lc) lc - o.lc
            else pid - o.pid
    }

    def entryAfter(le: LogEntry) = ???

#### Version Vector

I think this thing is just a `mutable.Map[BayouID, LC]`

And it goes a little-something like-a this:

    type LC = Int
    case class BayouID(LC, BayouID)
    val firstServer = BayouID(0, 0)   // see Bayou 2, pg. 295
    class VersionVector(myID: BayouID, server: Server) {
        val vv = mutable.Map.empty[BayouID, LC]
        def rcvCreationWrite(sender: ActorRef) = {
            val timestamp: LC = server.write(???)
            val newID: BayouID = BayouID(timestamp, myID)
            vv.put(newID, timestamp)
            sender ! YourBayouID(newID)
        }
    }

    /* "After issuing its creation write, the newly created server needs to
        perform anti-entropy with the server that just created it" */
    Server.receive = {
        case YourBayouID(theID) =>
            myBayouID = theID
            sender ! SendMeUpdates(myVersionVector)
        case Updates(theUpdates) =>
            theUpdates foreach log.write
    }

### Methods

#### Simple ones
1. `incrLogicalClock`
2. `incrClockFor(nodeID)`

#### Anti Entropy


#### Server Join
1. "Servers should follow the **recursive naming** procedure from the 2nd
   paper"

#### Retirement

### Primary Replica & Commitment
1. "Must be the first server in the system and upon retiring should hand off
   its duties to **the server** it informs of its retirement."


## Client (Actor)


# Processes Overview

## joinClient (clientID, serverID)

**Done**

1. The CLI calls `joinClient(c,s)` on Master
2. Master calls `Client.main(Array.empty)`
3. Master sends `ClusterKing` a `case class JoinClient(clientID, serverID)`
4. The `ClusterKing` stores the pair in a `var` or whatever
5. The `object Client` creates a `class Client Actor` who joins the Cluster
6. The `ClusterKing` receives the `MemberUp(m)` notification
7. It retrieves the saved id-pair
8. It saves the `Client`'s `ActorPath` in a field `nodes: Map[PID, ActorPath]`
9. It does `clientRef ! ConnectToPath(nodes(serverID))`
10. The `Client` stores the server in `server`

## joinServer(id)
**Done**

## Put Song
1. The `Client` does `server ! Put(song)`
2. The `Server` increments its Logical Clock
3. It appends the `Write(currentTimestamp, song)` to its `writeLog`
4. **TODO** does it execute right now?

## Program Termination
Since there's just a single JVM, I can just call `System exit 0`

## Retire Server
1. `Master Actor` receives `RetireServer(id)` from `object Master`
2. It forwards it to the appropriate server
3. The server calls `retire()`
4. It writes the retire entry
5. It initiates a Anti-Entropy with
