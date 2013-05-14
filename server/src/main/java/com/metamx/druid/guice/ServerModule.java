package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.druid.initialization.DruidNodeConfig;

/**
 */
public class ServerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid", DruidNodeConfig.class);
  }
}
