package com.metamx.druid.curator;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.guice.ConfigProvider;
import com.metamx.druid.guice.LazySingleton;
import com.metamx.druid.initialization.Initialization;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

/**
 */
public class CuratorModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, CuratorConfig.class);
  }

  @Provides @LazySingleton
  public CuratorFramework makeCurator(CuratorConfig config, Lifecycle lifecycle) throws IOException
  {
    return Initialization.makeCuratorFramework(config, lifecycle);
  }
}
