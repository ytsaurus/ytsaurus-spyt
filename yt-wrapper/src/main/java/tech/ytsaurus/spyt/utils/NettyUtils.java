package tech.ytsaurus.spyt.utils;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import tech.ytsaurus.client.bus.DefaultBusConnector;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;

public class NettyUtils {
    public static DefaultBusConnector createBusConnector(
        int nThreads,
        boolean useJobProxy,
        Duration timeout,
        ThreadFactory daemonThreadFactory
    ) {
        EventLoopGroup group = useJobProxy
            ? new EpollEventLoopGroup(nThreads, daemonThreadFactory)
            : new NioEventLoopGroup(nThreads, daemonThreadFactory);

        return new DefaultBusConnector(group, true)
            .setReadTimeout(timeout)
            .setWriteTimeout(timeout);
    }
}
