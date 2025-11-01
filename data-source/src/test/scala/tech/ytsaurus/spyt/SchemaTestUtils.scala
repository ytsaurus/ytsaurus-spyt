package tech.ytsaurus.spyt

import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField}
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields

trait SchemaTestUtils {
  def structField(name: String, dataType: DataType,
                  originalName: Option[String] = None,
                  keyId: Long = -1,
                  metadata: MetadataBuilder = new MetadataBuilder(),
                  nullable: Boolean = true,
                  arrowSupported: Boolean = true,
                  ytLogicalTypeName: String = null): StructField = {
    val metadataBuilder: MetadataBuilder = metadata
      .putString(MetadataFields.ORIGINAL_NAME, originalName.getOrElse(name))
      .putLong(MetadataFields.KEY_ID, keyId)
      .putBoolean(MetadataFields.ARROW_SUPPORTED, arrowSupported)

    if (ytLogicalTypeName != null) {
      metadataBuilder.putString(MetadataFields.YT_LOGICAL_TYPE, ytLogicalTypeName)
    }

    StructField(name, dataType, nullable = nullable, metadataBuilder.build())
  }
}
