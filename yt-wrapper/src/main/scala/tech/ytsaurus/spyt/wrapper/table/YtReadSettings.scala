package tech.ytsaurus.spyt.wrapper.table

/**
 * Settings for table reading operations.
 */
case class YtReadSettings(omitInaccessibleColumns: Boolean,
  omitInaccessibleRows: Boolean,
  distributedReadingEnabled: Boolean,
)

object YtReadSettings {
  val default: YtReadSettings = YtReadSettings(
    omitInaccessibleColumns = true,
    omitInaccessibleRows = true,
    distributedReadingEnabled = false,
  )
}
