package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.apache.spark.shuffle.ytsaurus.Config.*;
import static org.junit.jupiter.api.Assertions.*;

class ShuffleUtilsTest {
    @Test
    void rejectsSectionsOtherThanTheActiveMode() {
        for (boolean push : new boolean[]{false, true}) {
            String inactive = push ? "pull" : "push";
            for (String section : new String[]{inactive, "foo"}) {
                SparkConf conf = new SparkConf(false).set(YTSAURUS_SHUFFLE_CONFIG.key(),
                        "{" + section + "={writer={block_size=1024}}}");
                IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                        () -> ShuffleUtils.shuffleConfig(conf, push));
                assertEquals("requirement failed: " + YTSAURUS_SHUFFLE_CONFIG.key()
                        + " may contain only the '" + (push ? "push" : "pull")
                        + "' section when push-based shuffle is " + (push ? "enabled" : "disabled")
                        + ", got unexpected keys: " + section, error.getMessage());
            }
        }
    }

    @Test
    void acceptsActiveModeAndEmptyConfig() {
        for (boolean push : new boolean[]{false, true}) {
            for (String config : new String[]{"{}", "{" + (push ? "push" : "pull")
                    + "={writer={block_size=1024}}}"}) {
                SparkConf conf = new SparkConf(false).set(YTSAURUS_SHUFFLE_CONFIG.key(), config);
                assertEquals(YTreeTextSerializer.deserialize(config), ShuffleUtils.shuffleConfig(conf, push).get());
            }
        }
    }

    @Test
    void absentConfigIsOptional() {
        assertTrue(ShuffleUtils.shuffleConfig(new SparkConf(false), false).isEmpty());
        assertTrue(ShuffleUtils.shuffleConfig(new SparkConf(false), true).isEmpty());
    }
}
