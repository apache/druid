package com.metamx.druid.aggregation.post;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.Comparator;
import java.util.Map;

/**
 * Functionally similar to an Aggregator. See the Aggregator interface for more comments.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "arithmetic", value = ArithmeticPostAggregator.class),
    @JsonSubTypes.Type(name = "fieldAccess", value = FieldAccessPostAggregator.class),
    @JsonSubTypes.Type(name = "constant", value = ConstantPostAggregator.class)
})
public interface PostAggregator
{
  public Comparator getComparator();

  public Object compute(Map<String, Object> combinedAggregators);

  public String getName();
}
