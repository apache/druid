package com.metamx.druid.aggregation;

import com.google.common.primitives.Doubles;
import com.metamx.druid.processing.MetricSelectorFactory;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public class DoubleSumAggregatorFactory implements AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final String fieldName;
  private final String name;

  @JsonCreator
  public DoubleSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName.toLowerCase();
  }

  @Override
  public Aggregator factorize(MetricSelectorFactory metricFactory)
  {
    return new DoubleSumAggregator(
        name,
        metricFactory.makeFloatMetricSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(MetricSelectorFactory metricFactory)
  {
    return new DoubleSumBufferAggregator(metricFactory.makeFloatMetricSelector(fieldName));
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleSumAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return DoubleSumAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleSumAggregatorFactory(name, name);
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

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
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
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = fieldName.getBytes();

    return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "DoubleSumAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           '}';
  }
}
