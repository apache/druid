package io.druid.cli;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.curator.discovery.DiscoveryModule;
import com.metamx.druid.guice.CoordinatorModule;
import com.metamx.druid.guice.DbConnectorModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.JacksonConfigManagerModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.guice.ServerViewModule;
import com.metamx.druid.guice.annotations.Self;
import com.metamx.druid.http.InfoResource;
import com.metamx.druid.http.MasterResource;
import com.metamx.druid.http.RedirectFilter;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerInitializer;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.metrics.MetricsModule;
import io.airlift.command.Command;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;

/**
 */
@Command(
    name = "coordinator",
    description = "Runs the Coordinator, see https://github.com/metamx/druid/wiki/Master for a description."
)
public class CliCoordinator extends ServerRunnable
{
  private static final Logger log = new Logger(CliCoordinator.class);

  public CliCoordinator()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
        new LifecycleModule().register(DruidMaster.class),
        EmitterModule.class,
        HttpClientModule.global(),
        DbConnectorModule.class,
        JacksonConfigManagerModule.class,
        CuratorModule.class,
        new MetricsModule(),
        new DiscoveryModule().register(Self.class),
        ServerModule.class,
        new JettyServerModule(new CoordinatorJettyServerInitializer())
            .addResource(InfoResource.class)
            .addResource(MasterResource.class)
            .addResource(StatusResource.class),
        new ServerViewModule(),
        CoordinatorModule.class
    );
  }

  private static class CoordinatorJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      ResourceHandler resourceHandler = new ResourceHandler();
      resourceHandler.setResourceBase(DruidMaster.class.getClassLoader().getResource("static").toExternalForm());

      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.setContextPath("/");

      HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{resourceHandler, root, new DefaultHandler()});
      server.setHandler(handlerList);

      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(GzipFilter.class, "/*", null);
      root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);
      root.addFilter(GuiceFilter.class, "/*", null);
    }
  }
}
