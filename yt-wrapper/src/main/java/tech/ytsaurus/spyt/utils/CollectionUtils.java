package tech.ytsaurus.spyt.utils;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectionUtils {
    public static <K, T, U> Map<K, U> mapValues(Map<K, T> map, Function<T, U> mapper) {
        return map
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> mapper.apply(e.getValue())));
    }

    public static <K, V> Map<K, V> concatMaps(Map<K, ? extends V> map1, Map<K, ? extends V> map2) {
        return Stream.concat(map1.entrySet().stream(), map2.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static <K, V> Map<K, V> filterMap(Map<K, V> map, Predicate<? super Map.Entry<K, V>> filter) {
        return map.entrySet().stream()
            .filter(filter)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
