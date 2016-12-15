package io.druid.server.lookup.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

public class PrefetchKeyRangeProvider implements PrefetchKeyProvider
{
  @JsonProperty
  private final List<String> partitionKeys;

  private final NavigableSet<String> sortedPartitionKeys;
  private final PrefetchQueryProvider queryProvider;

  @JsonCreator
  public PrefetchKeyRangeProvider(
      @JsonProperty("partitionKeys") List<String> partitionKeys
  )
  {
    this.partitionKeys = Preconditions.checkNotNull(partitionKeys, "partitionKeys");
    this.sortedPartitionKeys = new TreeSet<>(this.partitionKeys);
    this.queryProvider = new PrefetchRangeQueryProvider();
  }

  @Override
  public PrefetchQueryProvider getQueryProvider()
  {
    return queryProvider;
  }

  @Override
  public String[] get(String key)
  {
    return new String[] {
        sortedPartitionKeys.floor(key),
        sortedPartitionKeys.higher(key)
    };
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PrefetchKeyRangeProvider)) {
      return false;
    }

    PrefetchKeyRangeProvider that = (PrefetchKeyRangeProvider) o;

    return sortedPartitionKeys.equals(that.sortedPartitionKeys);
  }
}
