package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.SparkConf;

import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import scala.Option;

import static org.apache.spark.shuffle.ytsaurus.Config.YTSAURUS_SHUFFLE_CONFIG;

public final class ShuffleUtils {
    private ShuffleUtils() { }

    static <T> Optional<T> toJavaOptional(Option<T> value) {
        return value.isDefined() ? Optional.ofNullable(value.get()) : Optional.empty();
    }

    public static Optional<YTreeMapNode> shuffleConfig(SparkConf conf, boolean pushBasedEnabled) {
        Optional<String> value = toJavaOptional(conf.get(YTSAURUS_SHUFFLE_CONFIG));
        if (value.isEmpty()) {
            return Optional.empty();
        }
        YTreeMapNode config = YTreeTextSerializer.deserialize(value.get()).mapNode();
        String activeSection = pushBasedEnabled ? "push" : "pull";
        List<String> unexpectedKeys = config.keys().stream()
                .filter(key -> !key.equals(activeSection))
                .collect(Collectors.toList());
        if (!unexpectedKeys.isEmpty()) {
            throw new IllegalArgumentException("requirement failed: " + YTSAURUS_SHUFFLE_CONFIG.key()
                    + " may contain only the '" + activeSection + "' section when push-based shuffle is "
                    + (pushBasedEnabled ? "enabled" : "disabled") + ", got unexpected keys: "
                    + String.join(", ", unexpectedKeys));
        }
        return Optional.of(config);
    }
}
