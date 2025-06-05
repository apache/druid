package org.apache.druid.server.http;

import com.google.inject.Inject;
import com.google.inject.Provider;

import javax.servlet.ServletContext;

import com.google.inject.Provides;
import org.apache.druid.server.AsyncQueryForwardingServlet;
import org.eclipse.jetty.util.thread.QueuedThreadPool;


public class RouterThreadPoolProvider implements Provider<QueuedThreadPool> {

    private final AsyncQueryForwardingServlet servlet;

    @Inject
    public RouterThreadPoolProvider(AsyncQueryForwardingServlet servlet) {
        this.servlet = servlet;
    }

    @Override
    @Provides
    public QueuedThreadPool get() {
        return this.servlet.getThreadPool();
    }
}
