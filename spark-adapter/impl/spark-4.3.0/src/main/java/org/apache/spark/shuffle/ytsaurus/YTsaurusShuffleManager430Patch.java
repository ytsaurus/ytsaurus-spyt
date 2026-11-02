package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.shuffle.BlockingShuffleManager;

import tech.ytsaurus.spyt.patch.annotations.AddInterfaces;
import tech.ytsaurus.spyt.patch.annotations.Applicability;
import tech.ytsaurus.spyt.patch.annotations.OriginClass;

@Applicability(from = "4.3.0")
@OriginClass("org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleManager")
@AddInterfaces({BlockingShuffleManager.class})
public class YTsaurusShuffleManager430Patch {
}
