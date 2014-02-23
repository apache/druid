package io.druid.query.aggregation.cardinality;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.NoopAggregator;
import io.druid.query.aggregation.NoopBufferAggregator;
import io.druid.query.aggregation.cardinality.hll.HyperLogLogPlus;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class DimensionCardinalityAggregatorFactory implements AggregatorFactory
{
  private static final byte[] CACHE_KEY = new byte[]{0x9};

  private final String name;
  private final String fieldName;

  @JsonCreator
  public DimensionCardinalityAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    Preconditions.checkNotNull(name, "must specify a name on DimensionCardinality aggregators");
    Preconditions.checkNotNull(fieldName, "must specify a fieldName on DimensionCardinality aggregators");

    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      return new NoopAggregator(name)
      {
        @Override
        public Object get()
        {
          return DimensionCardinalityAggregator.makeHllPlus();
        }
      };
    }

    return new DimensionCardinalityAggregator(name, selector);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      return new NoopBufferAggregator(){
        @Override
        public Object get(ByteBuffer buf, int position)
        {
          return DimensionCardinalityAggregator.makeHllPlus();
        }
      };
    }

    return new DimensionCardinalityBufferAggregator(selector);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o1, Object o2)
      {
        return Longs.compare(((HyperLogLogPlus) o1).cardinality(), ((HyperLogLogPlus) o2).cardinality());
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    HyperLogLogPlus leftHll = (HyperLogLogPlus) lhs;
    HyperLogLogPlus rightHll = (HyperLogLogPlus) rhs;

    if (leftHll.isReadOnly()) {
      if (rightHll.isReadOnly()) {
        HyperLogLogPlus retVal = leftHll.mutableCopy();
        retVal.addAll(rightHll);
        return retVal;
      }
      else {
        rightHll.addAll(leftHll);
        return rightHll;
      }
    }
    leftHll.addAll(rightHll);
    return leftHll;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DimensionCardinalityAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    final byte[] bytes;
    if (object instanceof String) {
      bytes = Base64.decodeBase64((String) object);
    }
    else if (object instanceof byte[]) {
      bytes = (byte[]) object;
    }
    else if (object instanceof HyperLogLogPlus) {
      return object;
    }
    else {
      throw new ISE("Cannot deserialize class[%s]", object.getClass());
    }

    return new HyperLogLogPlus(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()));
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((HyperLogLogPlus) object).cardinality();
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Lists.newArrayList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = fieldName.getBytes(Charsets.UTF_8);

    ByteBuffer buf = ByteBuffer.allocate(1 + fieldNameBytes.length);
    buf.put(CACHE_KEY);
    buf.put(fieldNameBytes);
    return buf.array();
  }

  @Override
  public String getTypeName()
  {
    return "hll+";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return DimensionCardinalityAggregator.MAX_SIZE_BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return DimensionCardinalityAggregator.makeHllPlus();
  }

  @Override
  public String toString()
  {
    return "DimensionCardinalityAggregatorFactory{" +
        "name='" + name + '\'' +
        ", fieldName='" + fieldName + '\'' +
        '}';
  }
}
