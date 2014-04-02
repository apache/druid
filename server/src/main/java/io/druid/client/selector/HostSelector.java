package io.druid.client.selector;

import com.metamx.common.Pair;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.query.Query;

/**
 */
public interface HostSelector<T>
{
  public String getDefaultServiceName();

  public Pair<String, ServerDiscoverySelector> select(Query<T> query);
}
