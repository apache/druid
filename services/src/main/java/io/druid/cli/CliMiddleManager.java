package io.druid.cli;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.guice.AWSModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.MiddleManagerModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.guice.TaskLogsModule;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.indexing.worker.WorkerTaskMonitor;
import com.metamx.druid.indexing.worker.http.WorkerResource;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerInitializer;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.metrics.MetricsModule;
import io.airlift.command.Command;
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
@Command(
    name = "middleManager",
    description = "Runs a Middle Manager, this is a \"task\" node used as part of the remote indexing service."
)
public class CliMiddleManager extends ServerRunnable
{
  private static final Logger log = new Logger(CliMiddleManager.class);

  public CliMiddleManager()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
        new LifecycleModule().register(WorkerTaskMonitor.class),
        EmitterModule.class,
        HttpClientModule.global(),
        CuratorModule.class,
        new MetricsModule(),
        ServerModule.class,
        new JettyServerModule(new MiddleManagerJettyServerInitializer())
            .addResource(StatusResource.class)
            .addResource(WorkerResource.class),
        new AWSModule(),
        new TaskLogsModule(),
        new MiddleManagerModule()
    );
  }

  private static class MiddleManagerJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(GzipFilter.class, "/*", null);
      root.addFilter(GuiceFilter.class, "/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root, new DefaultHandler()});
      server.setHandler(handlerList);
    }
  }
}
