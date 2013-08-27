package com.metamx.druid.curator;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.guice.ConfigProvider;
import com.metamx.druid.guice.LazySingleton;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;

import java.io.IOException;

/**
 */
public class CuratorModule implements Module
{
  private static final Logger log = new Logger(CuratorModule.class);

  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, CuratorConfig.class);
  }

  @Provides @LazySingleton
  public CuratorFramework makeCurator(CuratorConfig config, Lifecycle lifecycle) throws IOException
  {
    final CuratorFramework framework =
        CuratorFrameworkFactory.builder()
                               .connectString(config.getZkHosts())
                               .sessionTimeoutMs(config.getZkSessionTimeoutMs())
            .retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30))
            .compressionProvider(new PotentiallyGzippedCompressionProvider(config.enableCompression()))
            .build();

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            log.info("Starting Curator");
            framework.start();
          }

          @Override
          public void stop()
          {
            log.info("Stopping Curator");
            framework.close();
          }
        }
    );

    return framework;
  }
}
