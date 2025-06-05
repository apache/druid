package org.apache.druid.server.http;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.server.router.Router;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

public class RouterBindingModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(QueuedThreadPool.class)
                .annotatedWith(Router.class)
                .toProvider(RouterThreadPoolProvider.class)
                .in(LazySingleton.class);
        binder.bind(QueuedThreadPool.class)
                .annotatedWith(Global.class)
                .toProvider(GlobalThreadPoolProvider.class)
                .in(LazySingleton.class);
//        binder.bind(RouterThreadPoolProvider.class);
//        binder.bind(GlobalThreadPoolProvider.class);
//        MetricsModule.register(binder, JettyHttpClientMonitor.JettyRouterHttpClientMonitor.class);
//        MetricsModule.register(binder, JettyHttpClientMonitor.JettyGlobalHttpClientMonitor.class);
    }
}
