package com.metamx.druid.client.selector;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;

import java.util.LinkedHashSet;
import java.util.Random;

/**
 */
public class ServerSelector
{
  private static final Random random = new Random();

  private final LinkedHashSet<DruidServer> servers = Sets.newLinkedHashSet();
  private final DataSegment segment;

  public ServerSelector(
      DataSegment segment
  )
  {
    this.segment = segment;
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public void addServer(
      DruidServer server
  )
  {
    synchronized (this) {
      servers.add(server);
    }
  }

  public boolean removeServer(DruidServer server)
  {
    synchronized (this) {
      return servers.remove(server);
    }
  }

  public boolean isEmpty()
  {
    synchronized (this) {
      return servers.isEmpty();
    }
  }

  public DruidServer pick()
  {
    synchronized (this) {
      final int size = servers.size();
      switch (size) {
        case 0: return null;
        case 1: return servers.iterator().next();
        default: return Iterables.get(servers, random.nextInt(size));
      }
    }
  }
}
