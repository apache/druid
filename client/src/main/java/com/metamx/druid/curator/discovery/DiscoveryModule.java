package com.metamx.druid.curator.discovery;

import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.guice.JsonConfigProvider;
import com.metamx.druid.guice.LazySingleton;
import com.metamx.druid.initialization.CuratorDiscoveryConfig;
import com.metamx.druid.initialization.DruidNodeConfig;
import com.metamx.druid.initialization.Initialization;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;

/**
 */
public class DiscoveryModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.discovery.curator", CuratorDiscoveryConfig.class);
    binder.bind(ServiceAnnouncer.class).to(CuratorServiceAnnouncer.class).in(LazySingleton.class);
  }

  @Provides @LazySingleton
  public ServiceDiscovery<Void> getServiceDiscovery(
      CuratorFramework curator,
      Supplier<CuratorDiscoveryConfig> config,
      Lifecycle lifecycle
  ) throws Exception
  {
    return Initialization.makeServiceDiscoveryClient(curator, config.get(), lifecycle);
  }

  @Provides @LazySingleton
  public ServiceInstanceFactory<Void> getServiceInstanceFactory(
      Supplier<DruidNodeConfig> nodeConfig
  )
  {
    return Initialization.makeServiceInstanceFactory(nodeConfig.get());
  }
}
