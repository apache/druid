package com.metamx.druid.client.indexing;

import com.google.inject.Inject;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.selector.DiscoverySelector;
import com.metamx.druid.client.selector.Server;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;

import java.io.IOException;

/**
*/
public class IndexingServiceSelector implements DiscoverySelector<Server>
{
  private static final Logger log = new Logger(IndexingServiceSelector.class);

  private final ServiceProvider serviceProvider;

  @Inject
  public IndexingServiceSelector(
      @IndexingService ServiceProvider serviceProvider
  ) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public Server pick()
  {
    final ServiceInstance instance;
    try {
      instance = serviceProvider.getInstance();
    }
    catch (Exception e) {
      log.info(e, "");
      return null;
    }

    return new Server()
    {
      @Override
      public String getHost()
      {
        return instance.getAddress();
      }

      @Override
      public int getPort()
      {
        return instance.getPort();
      }
    };
  }

  @LifecycleStart
  public void start() throws Exception
  {
    serviceProvider.start();
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    serviceProvider.close();
  }
}
