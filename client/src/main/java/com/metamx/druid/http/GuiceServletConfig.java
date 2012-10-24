package com.metamx.druid.http;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;

/**
 */
public class GuiceServletConfig extends GuiceServletContextListener
{
  private final Injector injector;

  public GuiceServletConfig(
      Injector injector
  )
  {
    this.injector = injector;
  }
  
  @Override
  protected Injector getInjector()
  {
    return injector;
  }
}
