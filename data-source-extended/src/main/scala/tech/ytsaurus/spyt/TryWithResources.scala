package tech.ytsaurus.spyt

object TryWithResources {
  def apply[R <: AutoCloseable, A](resource: R)(body: R => A): A = TryWithResourcesJava.apply[R, A](resource, body(_))
}
