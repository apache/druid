package com.metamx.druid.initialization;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.guice.ConfigProvider;
import com.metamx.druid.guice.LazySingleton;
import org.eclipse.jetty.server.Server;

/**
 */
public class JettyServerModule implements Module
{
  private static final Logger log = new Logger(JettyServerModule.class);

  private final JettyServerInitializer initializer;

  public JettyServerModule(
      JettyServerInitializer initializer
  )
  {
    this.initializer = initializer;
  }

  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, ServerConfig.class);
  }

  @Provides @LazySingleton
  public Server getServer(Injector injector, Lifecycle lifecycle, ServerConfig config)
  {
    final Server server = Initialization.makeJettyServer(config);
    initializer.initialize(server, injector);
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            server.start();
          }

          @Override
          public void stop()
          {
            try {
              server.stop();
            }
            catch (Exception e) {
              log.warn(e, "Unable to stop Jetty server.");
            }
          }
        }
    );
    return server;
  }
}
