package com.metamx.druid.client.selector;

import com.metamx.druid.client.DirectDruidClient;
import com.metamx.druid.client.DruidServer;

/**
 */
public class QueryableDruidServer
{
  private final DruidServer server;
  private final DirectDruidClient client;

  public QueryableDruidServer(DruidServer server, DirectDruidClient client)
  {
    this.server = server;
    this.client = client;
  }

  public DruidServer getServer()
  {
    return server;
  }

  public DirectDruidClient getClient()
  {
    return client;
  }
}
