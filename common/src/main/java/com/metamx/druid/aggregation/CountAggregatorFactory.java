package com.metamx.druid.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.metamx.druid.processing.MetricSelectorFactory;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Comparator;
import java.util.List;

/**
 */
public class CountAggregatorFactory implements AggregatorFactory
{
  private static final byte[] CACHE_KEY = new byte[]{0x0};
  private final String name;

  @JsonCreator
  public CountAggregatorFactory(
      @JsonProperty("name") String name
  )
  {
    this.name = name;
  }

  @Override
  public Aggregator factorize(MetricSelectorFactory metricFactory)
  {
    return new CountAggregator(name);
  }

  @Override
  public BufferAggregator factorizeBuffered(MetricSelectorFactory metricFactory)
  {
    return new CountBufferAggregator();
  }

  @Override
  public Comparator getComparator()
  {
    return CountAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return CountAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongSumAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of();
  }

  @Override
  public byte[] getCacheKey()
  {
    return CACHE_KEY;
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "CountAggregatorFactory{" +
           "name='" + name + '\'' +
           '}';
  }
}
