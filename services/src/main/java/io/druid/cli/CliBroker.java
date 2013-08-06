package io.druid.cli;

import com.google.inject.Injector;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.guice.BrokerModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.QueryToolChestModule;
import com.metamx.druid.guice.QueryableModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.guice.ServerViewModule;
import com.metamx.druid.guice.annotations.Client;
import com.metamx.druid.http.ClientQuerySegmentWalker;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.metrics.MetricsModule;
import io.airlift.command.Command;

/**
 */
@Command(
    name = "broker",
    description = "Runs a broker node, see https://github.com/metamx/druid/wiki/Broker for a description"
)
public class CliBroker extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  public CliBroker()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
            new LifecycleModule(),
            EmitterModule.class,
            HttpClientModule.global(),
            CuratorModule.class,
            new MetricsModule(),
            ServerModule.class,
            new JettyServerModule(new QueryJettyServerInitializer())
                .addResource(StatusResource.class),
            new QueryableModule(ClientQuerySegmentWalker.class),
            new QueryToolChestModule(),
            new ServerViewModule(),
            new HttpClientModule("druid.broker.http", Client.class),
            new BrokerModule()
    );
  }
}
