package io.druid.cli;

import com.google.inject.Injector;
import com.metamx.common.logger.Logger;
import com.metamx.druid.coordination.ServerManager;
import com.metamx.druid.coordination.ZkCoordinator;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.guice.HistoricalModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.QueryRunnerFactoryModule;
import com.metamx.druid.guice.QueryableModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.metrics.MetricsModule;
import com.metamx.druid.metrics.ServerMonitor;
import io.airlift.command.Command;

/**
 */
@Command(
    name = "historical",
    description = "Runs a Historical node, see https://github.com/metamx/druid/wiki/Compute for a description"
)
public class CliHistorical extends ServerRunnable
{
  private static final Logger log = new Logger(CliHistorical.class);

  public CliHistorical()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
        new LifecycleModule().register(ZkCoordinator.class),
        EmitterModule.class,
        HttpClientModule.global(),
        CuratorModule.class,
        new MetricsModule().register(ServerMonitor.class),
        ServerModule.class,
        new JettyServerModule(new QueryJettyServerInitializer())
            .addResource(StatusResource.class),
        new QueryableModule(ServerManager.class),
        new QueryRunnerFactoryModule(),
        HistoricalModule.class
    );
  }
}
