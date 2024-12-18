package spyt

import sbt._
import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, releaseStepTask}
import sbtrelease.Utilities.stateW
import sbtrelease.Versions
import spyt.ReleaseUtils._
import spyt.SpytPlugin.autoImport._

object SpytSnapshot {

  lazy val spytSnapshotProcess: Seq[ReleaseStep] = Seq(
    ReleaseStep(releaseStepTask(prepareBuildDirectory)),
    spytSnapshotVersions,
    setSpytSnapshotVersion,
    releaseStepTask(spytUpdatePythonVersion),
    releaseStepTask(spytDistributive),
    releaseStepTask(publishToYt)
  )

  private lazy val spytSnapshotVersions: ReleaseStep = { st: State =>
    snapshotVersions(spytVersions, st, spytVersion)
  }

  private lazy val setSpytSnapshotVersion: ReleaseStep = {
    setVersion(spytVersions, Seq(
      spytVersion -> { v: Versions => v._1 },
      spytPythonVersion -> { v: Versions => v._2 }
    ), spytVersionFile)
  }

  private def snapshotVersions(versions: SettingKey[Versions],
                               st: State,
                               versionSetting: SettingKey[String]): State = {
    val rawCurVer = st.extract.get(versionSetting)
    st.log.info(s"Current raw version: $rawCurVer")

    val curVer = SpytVersion.parse(rawCurVer)
    st.log.info(s"Current version: ${curVer.toScalaString}")

    val newVer = curVer.inc
    st.log.info(s"New scala version: ${newVer.toScalaString}")
    st.log.info(s"New python version: ${newVer.toPythonString}")

    st.put(versions.key, (newVer.toScalaString, newVer.toPythonString))
  }
}
