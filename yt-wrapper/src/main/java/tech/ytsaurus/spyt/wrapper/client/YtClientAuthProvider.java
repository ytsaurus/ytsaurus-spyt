package tech.ytsaurus.spyt.wrapper.client;

import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

/** Loaded in the client JVM via a public no-argument constructor. */
@FunctionalInterface
public interface YtClientAuthProvider {
    /** Obtain credentials locally and apply any auth-specific settings to the supplied builder. */
    YTsaurusClientAuth getClientAuth(String configuration, YTsaurusClientConfig.Builder clientConfig);
}
