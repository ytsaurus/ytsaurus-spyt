package spyt

import scala.sys.process.{Process, ProcessLogger}
import scala.util.Random
import scala.util.control.NonFatal

sealed trait ReleaseSuffix {
  val scala: String
  val python: String
}

case object NoSuffix extends ReleaseSuffix {
  override val scala: String = ""
  override val python: String = ""
}

sealed abstract class NumberedReleaseSuffix(
  val scalaQ: String,
  val pythonQ: String,
  val sNumber: String) extends ReleaseSuffix {
  override val scala: String = s"-$scalaQ-$sNumber"
  override val python: String = s"$pythonQ$sNumber"
}

case class Alpha(override val sNumber: String) extends NumberedReleaseSuffix("alpha", "a", sNumber)
case class Beta(override val sNumber: String) extends NumberedReleaseSuffix("beta", "b", sNumber)
case class ReleaseCandidate(override val sNumber: String) extends NumberedReleaseSuffix("rc", "rc", sNumber)

object ReleaseSuffix {
  def apply(sType: String, sNumber: String): ReleaseSuffix = sType match {
    case "alpha" => Alpha(sNumber)
    case "beta" => Beta(sNumber)
    case "rc" => ReleaseCandidate(sNumber)
    case _ => NoSuffix
  }
}

case class SpytVersion(main: String,
                       suffix: ReleaseSuffix,
                       ticket: Int,
                       hash: Int,
                       dev: Int) {
  def toScalaString: String = {
    (ticket, hash, dev) match {
      case (0, 0, 0) => main + suffix.scala
      case _ => s"$main-$ticket-$hash-$dev-SNAPSHOT"
    }
  }

  def toPythonString: String = {
    (ticket, hash, dev) match {
      case (0, 0, 0) => main + suffix.python
      case _ => s"${main}b$ticket.post$hash.dev$dev"
    }
  }

  def inc: SpytVersion = getVcsInfo()
    .map(info => copy(dev = SpytVersion.generateDev(), hash = info.hash, ticket = info.ticketNumber))
    .get

  case class VcsInfo(rawHash: String, branch: String, isGit: Boolean) {
    val hash: Int = Integer.parseInt(rawHash.take(5), 36)

    def ticketNumber: Int = {
      val p = "^(.*/)?(\\w+)[ -_](\\d+)[^/]*$".r

      branch match {
        case p(_, _, n) => n.toInt
        case "trunk" | "main" => 1
        case _ => 0
      }
    }
  }

  private def getTeamcityBuildNumber: String = {
    sys.env.getOrElse("TC_BUILD", Random.alphanumeric.take(5).mkString)
  }

  private def getVcsInfo(submodule: String = ""): Option[VcsInfo] = {
    try {
      val catchStderr: ProcessLogger = new ProcessLogger {
        override def out(s: => String): Unit = ()
        override def err(s: => String): Unit = ()
        override def buffer[T](f: => T): T = f
      }
      val out = Process("arc info").lineStream(catchStderr).toList
      val m = out.map(_.split(':').toList).map(as => (as(0), as(1).trim)).toMap
      for {
        hash <- m.get("hash")
        branch <- m.get("branch")
      } yield VcsInfo(hash, branch, isGit = false)
    } catch {
      // arc not found or it is not arc branch
      // let's try git
      case NonFatal(_) =>
        for {
          branch <- gitBranch(submodule)
          hash <- gitHash(submodule)
        } yield VcsInfo(hash, branch, isGit = true)
    }
  }

  private def gitBranch(submodule: String = ""): Option[String] = {
    val cmd = "git rev-parse --abbrev-ref HEAD"
    val real = if (submodule.isEmpty) cmd else s"git submodule foreach $cmd -- $submodule"
    try Process(real).lineStream.headOption catch {
      case _: Throwable => None
    }
  }

  private def gitHash(submodule: String = ""): Option[String] = {
    val loc = if (submodule.isEmpty) "HEAD" else s"HEAD:$submodule"
    try Process(s"git rev-parse --short $loc").lineStream.headOption catch {
      case _: Throwable => None
    }
  }
}

object SpytVersion {
  private val snapshotVersionRegex = "^([0-9.]+)(-(\\d+))?(-(\\d+))?(-(\\d+))?-SNAPSHOT$".r
  private val releaseVersionRegex = "^(\\d+)\\.(\\d+)\\.(\\d+)(-(alpha|beta|rc)-(\\d+))?$".r
  private val pythonVersionRegex = "^([0-9.]+)(b(\\d+))?(\\.post(\\d+))?(\\.dev(\\d+))?$".r
  private val defaultTicket = 0
  private val defaultHash = 0
  private val defaultDev = 0

  private def generateDev(): Int = {
    (System.currentTimeMillis() / 10000).toInt
  }

  private def intOrDefault(str: String, default: Int): Int = {
    Option(str).filter(_.nonEmpty).map(_.toInt).getOrElse(default)
  }

  private def parseTicket(str: String): Int = intOrDefault(str, defaultTicket)
  private def parseHash(str: String): Int = intOrDefault(str, defaultHash)
  private def parseDev(str: String): Int = intOrDefault(str, defaultDev)

  def parse(str: String): SpytVersion = {
    str match {
      case snapshotVersionRegex(main, _, ticket, _, hash, _, dev) =>
        SpytVersion(
          main, NoSuffix, parseTicket(ticket), parseHash(hash), parseDev(dev)
        )
      case pythonVersionRegex(main, _, ticket, _, hash, _, dev) =>
        SpytVersion(
          main, NoSuffix, parseTicket(ticket), parseHash(hash), parseDev(dev)
        )
      case releaseVersionRegex(major, minor, bugfix, _, sType, sNumber) =>
        val releaseSuffix = ReleaseSuffix(sType, sNumber)
        SpytVersion(
          s"$major.$minor.$bugfix", releaseSuffix, defaultTicket, defaultHash, defaultDev
        )
      case _ =>
        println(s"Unable to parse version $str")
        throw new IllegalArgumentException(s"Unable to parse version $str")
    }
  }
}