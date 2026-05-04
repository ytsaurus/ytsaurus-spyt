package tech.ytsaurus.spyt.args

case class ArgsException(message: String) extends RuntimeException(message)

/**
 * The args class does a simple command line parsing. The rules are:
 * keys start with one or more "-". Each key has zero or more values
 * following.
 */
object Args {
  /**
   * Split on whitespace and then parse.
   */
  def apply(argString: String): Args = Args(argString.split("\\s+"))

  /**
   * parses keys as starting with a dash, except single dashed digits.
   * All following non-dashed args are a list of values.
   * If the list starts with non-dashed args, these are associated with the
   * empty string: ""
   */
  def apply(args: Iterable[String]): Args = {
    new Args(
      //Fold into a list of (arg -> List[values])
      args
        .filter{ a => !a.matches("\\s*") }
        .foldLeft(List("" -> List[String]())) { (acc, arg) =>
          val noDashes = arg.dropWhile{ _ == '-' }
          if (arg == noDashes || isNumber(arg))
            (acc.head._1 -> (arg :: acc.head._2)) :: acc.tail
          else
            (noDashes -> List()) :: acc
        }
        //Now reverse the values to keep the same order
        .map { case (key, value) => key -> value.reverse }.toMap)
  }

  private def isNumber(arg: String): Boolean = {
    try {
      arg.toDouble
      true
    } catch {
      case _: NumberFormatException => false
    }
  }
}

class Args(val m: Map[String, List[String]]) extends java.io.Serializable {

  //Replace or add a given key+args pair:
  def +(keyvals: (String, Iterable[String])): Args = new Args(m + (keyvals._1 -> keyvals._2.toList))

  /**
   * Does this Args contain a given key?
   */
  def boolean(key: String): Boolean = m.contains(key)

  /**
   * Get the list of values associated with a given key.
   * if the key is absent, return the empty list.  NOTE: empty
   * does not mean the key is absent, it could be a key without
   * a value.  Use boolean() to check existence.
   */
  def list(key: String): List[String] = m.getOrElse(key, List())

  /**
   * This is a synonym for required
   */
  def apply(key: String): String = required(key)

  /**
   * Gets the list of positional arguments
   */
  private def positional: List[String] = list("")

  /**
   * return required positional value.
   */
  def required(position: Int): String = positional match {
    case l if l.size > position => l(position)
    case _ => throw ArgsException("Please provide " + (position + 1) + " positional arguments")
  }

  /**
   * This is a synonym for required
   */
  def apply(position: Int): String = required(position)

  override def equals(other: Any): Boolean = other match {
    case args: Args => args.m.equals(m)
    case _ => false
  }

  override def hashCode(): Int = m.hashCode()

  /**
   * Equivalent to .optional(key).getOrElse(default)
   */
  def getOrElse(key: String, default: String): String = optional(key).getOrElse(default)

  /**
   * return exactly one value for a given key.
   * If there is more than one value, you get an exception
   */
  def required(key: String): String = list(key) match {
    case List() => throw ArgsException("Please provide a value for --" + key)
    case List(a) => a
    case _ => throw ArgsException("Please only provide a single value for --" + key)
  }

  def toList: List[String] = {
    m.foldLeft(List[String]()) { (args, kvlist) =>
      val k = kvlist._1
      val values = kvlist._2
      if (k != "") {
        //Make sure positional args are first
        args ++ (("--" + k) :: values)
      } else {
        // These are positional args (no key), put them first:
        values ++ args
      }
    }
  }

  override def toString: String = toList.mkString(" ")

  /**
   * If there is zero or one element, return it as an Option.
   * If there is a list of more than one item, you get an error
   */
  def optional(key: String): Option[String] = list(key) match {
    case List() => None
    case List(a) => Some(a)
    case _ => throw ArgsException("Please provide at most one value for --" + key)
  }
}
