package org.apache.druid.server.http;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.server.router.Router;
import org.eclipse.jetty.util.thread.QueuedThreadPool;


public class JettyHttpClientMonitor extends AbstractMonitor {

    private final String prefix;
    private final QueuedThreadPool threadPool;
//    private final JettyHttpClientModule.HttpClientProvider httpClientProvider;


    public JettyHttpClientMonitor(QueuedThreadPool threadPool, String prefix) {
        this.prefix = prefix;
        this.threadPool = threadPool;
    }

    @Override
    public boolean doMonitor(ServiceEmitter emitter) {
        if (threadPool == null) {
            return true;
        }
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
        emitter.emit(builder.setMetric(prefix + "/jetty/threadPool/total", threadPool.getThreads()));
        emitter.emit(builder.setMetric(prefix + "/jetty/threadPool/idle", threadPool.getIdleThreads()));
        emitter.emit(builder.setMetric(prefix + "/jetty/threadPool/busy", threadPool.getBusyThreads()));
        emitter.emit(builder.setMetric(prefix + "/jetty/threadPool/queueSize", threadPool.getQueueSize()));
        emitter.emit(builder.setMetric(prefix + "/jetty/threadPool/maxThreads", threadPool.getMaxThreads()));
        return true;

    }

    public static class JettyRouterHttpClientMonitor extends JettyHttpClientMonitor {
        @Inject
        public JettyRouterHttpClientMonitor(@Router QueuedThreadPool threadPool) {
            super(threadPool, "router");
        }
    }

    public static class JettyGlobalHttpClientMonitor extends JettyHttpClientMonitor {
        @Inject
        public JettyGlobalHttpClientMonitor(@Global QueuedThreadPool threadPool) {
            super(threadPool, "global");
        }
    }
}