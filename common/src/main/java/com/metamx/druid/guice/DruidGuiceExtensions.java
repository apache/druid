package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;

/**
 */
public class DruidGuiceExtensions implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bindScope(LazySingleton.class, DruidScopes.SINGLETON);
  }
}
