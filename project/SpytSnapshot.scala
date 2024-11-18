package spyt

import sbt._
import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, releaseStepTask}
import sbtrelease.Utilities.stateW
import sbtrelease.Versions
import spyt.ReleaseUtils._
import spyt.SpytPlugin.autoImport._

import scala.sys.process.{Process, ProcessLogger}
import scala.util.Random
import scala.util.control.NonFatal

object SpytSnapshot {

  lazy val spytSnapshotProcess: Seq[ReleaseStep] = Seq(
    ReleaseStep(releaseStepTask(prepareBuildDirectory)),
    spytSnapshotVersions,
    setSpytSnapshotVersion,
    releaseStepTask(spytUpdatePythonVersion),
    releaseStepTask(spytDistributive),
    releaseStepTask(publishToYt)
  )

  case class SnapshotVersion(main: String,
                             ticket: Int,
                             hash: Int,
                             dev: Int) {
    def toScalaString: String = {
      (ticket, hash, dev) match {
        case (0, 0, 0) => main
        case _ => s"$main-$ticket-$hash-$dev-SNAPSHOT"
      }
    }

    def toPythonString: String = {
      (ticket, hash, dev) match {
        case (0, 0, 0) => main
        case _ => s"${main}b$ticket.post$hash.dev$dev"
      }
    }

    def inc: SnapshotVersion = getVcsInfo()
        .map(info => copy(dev = SnapshotVersion.generateDev(), hash = info.hash, ticket = info.ticketNumber))
        .get

    def updateDev(other: Option[SnapshotVersion]): SnapshotVersion = {
      other.map(o => copy(dev = dev.max(o.dev))).getOrElse(this)
    }

    case class VcsInfo(rawHash: String, branch: String, isGit: Boolean) {
      val hash: Int = Integer.parseInt(rawHash.take(5), 36)

      def ticketNumber: Int = {
        val p = "^(.*/)?(\\w+)[ -_](\\d+)[^/]*$".r

        branch match {
          case p(_, _, n) => n.toInt
          case "develop" => 1
          case "trunk" => 1
          case "teamcity" => 1
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

  object SnapshotVersion {
    private val snapshotVersionRegex = "^([0-9.]+)(-(\\d+))?(-(\\d+))?(-(\\d+))?-SNAPSHOT$".r
    private val releaseVersionRegex = "^(\\d+)\\.(\\d+)\\.(\\d+)$".r
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

    def parse(str: String): SnapshotVersion = {
      str match {
        case snapshotVersionRegex(main, _, ticket, _, hash, _, dev) =>
          SnapshotVersion(
            main, parseTicket(ticket), parseHash(hash), parseDev(dev)
          )
        case pythonVersionRegex(main, _, ticket, _, hash, _, dev) =>
          SnapshotVersion(
            main, parseTicket(ticket), parseHash(hash), parseDev(dev)
          )
        case releaseVersionRegex(major, minor, bugfix) =>
          SnapshotVersion(
            s"$major.$minor.${bugfix.toInt + 1}", parseTicket(""), parseHash(""), parseDev("")
          )
        case _ =>
          println(s"Unable to parse version $str")
          throw new IllegalArgumentException(s"Unable to parse version $str")
      }
    }

    def latestPublishedPython(log: Logger, pythonRegistry: String, packageName: String): Option[SnapshotVersion] = {
      PypiUtils.latestVersion(log, pythonRegistry, packageName).map {
        case pythonVersionRegex(main, _, ticket, _, hash, _, dev) =>
            SnapshotVersion(main, parseTicket(ticket), parseHash(hash), parseDev(dev))
      }
    }
  }

  private lazy val spytSnapshotVersions: ReleaseStep = { st: State =>
    snapshotVersions(spytVersions, st, spytVersion, "ytsaurus-spyt")
  }
  private lazy val setSpytSnapshotVersion: ReleaseStep = {
    setVersion(spytVersions, Seq(
      spytVersion -> { v: Versions => v._1 },
      spytPythonVersion -> { v: Versions => v._2 }
    ), spytVersionFile)
  }


  private def snapshotVersions(versions: SettingKey[Versions],
                               st: State,
                               versionSetting: SettingKey[String],
                               pythonPackage: String): State = {
    val rawCurVer = st.extract.get(versionSetting)
    st.log.info(s"Current raw version: $rawCurVer")

    val curVer = SnapshotVersion.parse(rawCurVer)
    st.log.info(s"Current version: ${curVer.toScalaString}")

    val latestPythonVer = SnapshotVersion.latestPublishedPython(st.log, st.extract.get(pypiRegistry), pythonPackage)
    val newVer = curVer.updateDev(latestPythonVer).inc
    st.log.info(s"New scala version: ${newVer.toScalaString}")
    st.log.info(s"New python version: ${newVer.toPythonString}")

    st.put(versions.key, (newVer.toScalaString, newVer.toPythonString))
  }

  private def snapshotVersion(versions: SettingKey[Versions],
                              st: State,
                              versionSetting: SettingKey[String]): State = {
    val rawCurVer = st.extract.get(versionSetting)
    st.log.info(s"Current raw version: $rawCurVer")

    val curVer = SnapshotVersion.parse(rawCurVer)
    st.log.info(s"Current version: ${curVer.toScalaString}")

    val newVer = curVer.inc
    st.log.info(s"New scala version: ${newVer.toScalaString}")

    st.put(versions.key, (newVer.toScalaString, ""))
  }
}
