package io.druid.server.bridge;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import io.druid.server.initialization.JettyServerInitializer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;

/**
 */
public class BridgeJettyServerInitializer implements JettyServerInitializer
{
  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);

    ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);

    root.addServlet(holderPwd, "/");
    //root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);
    root.addFilter(GzipFilter.class, "/*", null);

    // Can't use '/*' here because of Guice and Jetty static content conflicts
    // The coordinator really needs a standarized api path
    root.addFilter(GuiceFilter.class, "/status/*", null);

    HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{root});

    server.setHandler(handlerList);
  }
}
