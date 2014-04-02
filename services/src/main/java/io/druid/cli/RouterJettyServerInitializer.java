package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.RoutingDruidClient;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.server.AsyncQueryForwardingServlet;
import io.druid.server.QueryIDProvider;
import io.druid.server.initialization.JettyServerInitializer;
import io.druid.server.log.RequestLogger;
import io.druid.server.router.QueryHostFinder;
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
public class RouterJettyServerInitializer implements JettyServerInitializer
{
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final RoutingDruidClient routingDruidClient;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final QueryIDProvider idProvider;

  @Inject
  public RouterJettyServerInitializer(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      RoutingDruidClient routingDruidClient,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryIDProvider idProvider
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.routingDruidClient = routingDruidClient;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.idProvider = idProvider;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler queries = new ServletContextHandler(ServletContextHandler.SESSIONS);
    queries.addServlet(
        new ServletHolder(
            new AsyncQueryForwardingServlet(
                jsonMapper,
                smileMapper,
                hostFinder,
                routingDruidClient,
                emitter,
                requestLogger,
                idProvider
            )
        ), "/druid/v2/*"
    );
    queries.addFilter(GzipFilter.class, "/druid/v2/*", null);
    //queries.addFilter(GuiceFilter.class, "/druid/v2/*", null);

    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    root.addFilter(GzipFilter.class, "/*", null);
    root.addFilter(GuiceFilter.class, "/*", null);

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{queries, root, new DefaultHandler()});
    server.setHandler(handlerList);
  }
}
