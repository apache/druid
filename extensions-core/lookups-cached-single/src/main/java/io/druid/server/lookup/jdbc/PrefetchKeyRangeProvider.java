package io.druid.server.lookup.jdbc;

import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.Pair;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

public class PrefetchKeyRangeProvider implements PrefetchKeyProvider
{
  @JsonProperty
  private final List<String> partitionKeys;

  private final NavigableSet<String> sortedPartitionKeys;

  @JsonCreator
  public PrefetchKeyRangeProvider(
      @JsonProperty("partitionKeys") List<String> partitionKeys
  )
  {
    this.partitionKeys = Preconditions.checkNotNull(partitionKeys, "partitionKeys");
    this.sortedPartitionKeys = new TreeSet<>(this.partitionKeys);
  }

  @Override
  public ReturnType getReturnType()
  {
    return ReturnType.Range;
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

    return CollectionUtils.isEqualCollection(sortedPartitionKeys, that.sortedPartitionKeys);
  }
}
