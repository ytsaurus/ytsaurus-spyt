package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.DataSource

object AdapterSupport430 {

  def checkAndGlobPathIfNecessary(paths: Seq[String], hadoopConf: Configuration): Seq[Path] = {
    DataSource.checkAndGlobPathIfNecessary(paths, hadoopConf,
      checkEmptyGlobPath = true, checkFilesExist = true, enableGlobbing = true)
  }

}
