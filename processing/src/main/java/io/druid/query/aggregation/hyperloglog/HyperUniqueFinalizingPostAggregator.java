package io.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class HyperUniqueFinalizingPostAggregator implements PostAggregator
{
  private final String fieldName;

  @JsonCreator
  public HyperUniqueFinalizingPostAggregator(
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.fieldName = fieldName;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return HyperUniquesAggregatorFactory.estimateCardinality(combinedAggregators.get(fieldName));
  }

  @Override
  @JsonProperty("fieldName")
  public String getName()
  {
    return fieldName;
  }
}
