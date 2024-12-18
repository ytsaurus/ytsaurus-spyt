package spyt

import sbtrelease.ReleasePlugin.autoImport._
import spyt.ReleaseUtils._
import spyt.SpytPlugin.autoImport._

object SpytRelease {

  lazy val spytReleaseProcess: Seq[ReleaseStep] = Seq(
    ReleaseStep(releaseStepTask(prepareBuildDirectory)),
    setCustomSpytVersions,
    ReleaseStep(releaseStepTask(spytUpdatePythonVersion)),
    ReleaseStep(releaseStepTask(spytDistributive)),
    dumpVersions
  )

  private lazy val setCustomSpytVersions: ReleaseStep = {
    customSpytVersion.map { v =>
      val sv = SpytVersion.parse(v)
      setVersionForced(Seq(
        spytVersion -> sv.toScalaString,
        spytPythonVersion -> sv.toPythonString
      ), spytVersionFile)
    }.getOrElse(ReleaseStep(identity))
  }
}
