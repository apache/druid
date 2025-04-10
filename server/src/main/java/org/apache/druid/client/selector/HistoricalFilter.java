package org.apache.druid.client.selector;

import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.QueryableDruidServer;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class HistoricalFilter implements Function<Int2ObjectRBTreeMap<Set<QueryableDruidServer>>, Int2ObjectRBTreeMap<Set<QueryableDruidServer>>>
{
  public static HistoricalFilter IDENTITIY_FILTER = new HistoricalFilter(ImmutableSet::of);

  public HistoricalFilter(Supplier<Set<String>> serversToIgnoreSupplier)
  {
    this.serversToIgnoreSupplier = serversToIgnoreSupplier;
  }

  private final Supplier<Set<String>> serversToIgnoreSupplier;

  @Override
  public Int2ObjectRBTreeMap<Set<QueryableDruidServer>> apply(Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers)
  {
    final Set<String> serversToIgnore = serversToIgnoreSupplier.get();
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filteredHistoricals = new Int2ObjectRBTreeMap<>();
    for (int priority : historicalServers.keySet()) {
      Set<QueryableDruidServer> servers = historicalServers.get(priority);
      filteredHistoricals.put(priority,
                              servers.stream()
                                     .filter(server -> !serversToIgnore.contains(server.getServer().getHost()))
                                     .collect(Collectors.toSet())
      );
    }

    return filteredHistoricals;
  }
}
