package com.metamx.druid.initialization;

import com.google.inject.Injector;
import org.eclipse.jetty.server.Server;

/**
 */
public interface JettyServerInitializer
{
  public void initialize(Server server, Injector injector);
}
