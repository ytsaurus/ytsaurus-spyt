package org.apache.spark.launcher;

import tech.ytsaurus.spyt.patch.annotations.Applicability;
import tech.ytsaurus.spyt.patch.annotations.OriginClass;
import tech.ytsaurus.spyt.patch.annotations.Subclass;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.launcher.CommandBuilderUtils.findJarsDir;

/**
 * Patches:
 * 1. Added SPYT_CLASSPATH env variable value to classpath of jvm Spark components.
 */
@Subclass
@OriginClass("org.apache.spark.launcher.AbstractCommandBuilder")
abstract class AbstractCommandBuilderForSpyt extends AbstractCommandBuilder {

    @Override
    List<String> buildClassPath(String appClassPath) throws IOException {
        if (isSpytTesting()) {
            return List.of(System.getProperty("java.class.path").split(File.pathSeparator));
        }

        List<String> classPath = super.buildClassPath(appClassPath);

        // Spyt classpath should have precedence over spark class path, but not over conf directory,
        // so here we're searching for sparkJarsDir and inserting spyt classpath right before it.
        String sparkJarsDir = findJarsDir(getSparkHome(), getScalaVersion(), false);
        int sparkClasspathPos = 0;
        while (sparkClasspathPos < classPath.size() &&
                !classPath.get(sparkClasspathPos).contains(sparkJarsDir)) {
            sparkClasspathPos++;
        }
        classPath.add(sparkClasspathPos, getenv("SPYT_CLASSPATH"));
        String sparkConnectClasspath = System.getenv("SPARK_CONNECT_CLASSPATH");
        if (sparkConnectClasspath != null) {
            classPath.add(sparkConnectClasspath);
        }

        return classPath;
    }

    @Override
    List<String> buildJavaCommand(String extraClassPath) throws IOException {
        List<String> cmd = super.buildJavaCommand(extraClassPath);
        if (isSpytTesting()) {
            RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
            List<String> jvmArgs = bean.getInputArguments();
            List<String> extraParameters = jvmArgs.stream().flatMap(arg -> {
                if (arg.startsWith("-javaagent:")) {
                    return Stream.of(arg);
                } else if (arg.startsWith("-agentlib:jdwp")) {
                    return Stream.of(takeFreeJdwpPort(arg));
                } else {
                    return Stream.empty();
                }
            }).collect(Collectors.toList());
            cmd.addAll(1, extraParameters);
        }
        return cmd;
    }

    private boolean isSpytTesting() {
        return System.getenv("SPYT_TESTING") != null;
    }

    private String takeFreeJdwpPort(String arg) {
        // a very simple but very cringe algorithm for setting jdwp ports for executors
        int portPosition = arg.lastIndexOf(":") + 1;
        int port = Integer.parseInt(arg.substring(portPosition)) + portDelta;
        portDelta += 2;
        return arg.substring(0, portPosition) + port;
    }

    private static int portDelta = 2;
}
