package org.apache.druid.server.coordinator;

import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class Servers
{
  private static final AtomicLong SERVER_ID = new AtomicLong(0);

  private final ServerType type;
  private final Set<DataSegment> segments = new HashSet<>();

  private String name;
  private String hostAndPort;
  private String tier = DruidServer.DEFAULT_TIER;

  /**
   * Default size of 10GB.
   */
  private long size = 10L << 30;

  private boolean decommissioning = false;

  private Servers(ServerType type)
  {
    this.type = type;
  }

  public static Servers historical()
  {
    return new Servers(ServerType.HISTORICAL);
  }

  public static Servers broker()
  {
    return new Servers(ServerType.BROKER);
  }

  public Servers in(String tier)
  {
    this.tier = tier;
    return this;
  }

  public Servers ofName(String name)
  {
    this.name = name;
    return this;
  }

  public Servers withHostAndPort(String hostAndPort)
  {
    this.hostAndPort = hostAndPort;
    return this;
  }

  public Servers inDecommissioningMode()
  {
    this.decommissioning = true;
    return this;
  }

  public Servers withSegments(DataSegment... segments)
  {
    return withSegments(Arrays.asList(segments));
  }

  public Servers withSegments(Collection<DataSegment> segments)
  {
    this.segments.addAll(segments);
    return this;
  }

  public Servers ofSizeInGb(long sizeInGb)
  {
    this.size = sizeInGb << 30;
    return this;
  }

  public DruidServer in()
  {
    name = name == null ? type.name() + "_" + SERVER_ID.incrementAndGet() : name;
    hostAndPort = hostAndPort == null ? name : hostAndPort;

    final DruidServer server = new DruidServer(name, hostAndPort, null, size, type, tier, 1);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }

    return server;
  }

  public ImmutableDruidServer asImmutable()
  {
    return in().toImmutableDruidServer();
  }

  public ServerHolder asHolder()
  {
    return new ServerHolder(
        asImmutable(),
        new TestLoadQueuePeon(),
        decommissioning
    );
  }
}
