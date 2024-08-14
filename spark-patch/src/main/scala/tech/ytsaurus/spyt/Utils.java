package tech.ytsaurus.spyt;

public class Utils {
    private static boolean isIpV6Host(String host) {
        return host != null && host.contains(":");
    }

    public static String removeBracketsIfIpV6Host(String host) {
        if (isIpV6Host(host) && host.startsWith("[")) {
            return host.substring(1, host.length() - 1);
        } else {
            return host;
        }
    }

    public static String addBracketsIfIpV6Host(String host) {
        if (isIpV6Host(host) && !host.startsWith("[")) {
            return "[" + host + "]";
        } else {
            return host;
        }
    }
}
