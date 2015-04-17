val a: PartialFunction[Any, Any] = {
    case any ⇒
        println(s"$any")
        any
}

val b: PartialFunction[Any, Unit] = {
    case any ⇒
        println(s"$any again")
}

def c: PartialFunction[Any, Unit] = a andThen b

c(3)
