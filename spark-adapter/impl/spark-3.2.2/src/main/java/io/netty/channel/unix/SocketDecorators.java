package io.netty.channel.unix;

import tech.ytsaurus.spyt.patch.annotations.Decorate;
import tech.ytsaurus.spyt.patch.annotations.DecoratedMethod;
import tech.ytsaurus.spyt.patch.annotations.OriginClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;

@Decorate
@OriginClass("io.netty.channel.unix.Socket")
public class SocketDecorators {

    @DecoratedMethod
    public final boolean connect(SocketAddress socketAddress) throws IOException {
        try {
            return __connect(socketAddress);
        } catch (ConnectException | FileNotFoundException e) {
            // We can't connect to rpc job proxy via unix socket. Probably, there's an issue with the proxy itself.
            // Hence we immediately stop this process because it became useless.
            System.exit(1);
            // throw e is needed because otherwise it won't compile.
            throw e;
        }
    }

    public final boolean __connect(SocketAddress socketAddress) throws IOException {
        throw new RuntimeException("Must be replaced with original method");
    }
}
