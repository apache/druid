package org.apache.druid.msq.input.table;

import org.apache.druid.server.coordination.DruidServerMetadata;
import org.jboss.netty.util.internal.ThreadLocalRandom;

import java.util.Set;
import java.util.function.Function;

public enum DataServerSelector
{
  RANDOM(servers -> servers.stream()
                           .skip(ThreadLocalRandom.current().nextInt(servers.size()))
                           .findFirst()
                           .orElse(null));

  private final Function<Set<DruidServerMetadata>, DruidServerMetadata> selectServer;

  DataServerSelector(Function<Set<DruidServerMetadata>, DruidServerMetadata> selectServer)
  {
    this.selectServer = selectServer;
  }

  public Function<Set<DruidServerMetadata>, DruidServerMetadata> getSelectServerFunction()
  {
    return selectServer;
  }
}
