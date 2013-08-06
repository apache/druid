package io.druid.cli;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.druid.http.QueryServlet;
import com.metamx.druid.initialization.JettyServerInitializer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;

/**
*/
public class QueryJettyServerInitializer implements JettyServerInitializer
{
  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler queries = new ServletContextHandler(ServletContextHandler.SESSIONS);
    queries.setResourceBase("/");
    queries.addServlet(new ServletHolder(injector.getInstance(QueryServlet.class)), "/druid/v2/*");

    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    root.addFilter(GzipFilter.class, "/*", null);
    root.addFilter(GuiceFilter.class, "/*", null);

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{queries, root, new DefaultHandler()});
    server.setHandler(handlerList);
  }
}
