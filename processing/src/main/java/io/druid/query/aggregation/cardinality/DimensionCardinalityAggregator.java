package io.druid.query.aggregation.cardinality;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Throwables;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class DimensionCardinalityAggregator implements Aggregator
{
  static final int MAX_SIZE_BYTES = 1381;

  static final HyperLogLogPlus makeHllPlus()
  {
    return new HyperLogLogPlus(11, 0);
  }

  static final HyperLogLogPlus fromBytes(byte[] bytes)
  {
    try {
      return HyperLogLogPlus.Builder.build(bytes);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private final String name;
  private final ObjectColumnSelector selector;

  private volatile HyperLogLogPlus hllPlus = null;

  public DimensionCardinalityAggregator(
      String name,
      ObjectColumnSelector selector
  )
  {
    this.name = name;
    this.selector = selector;

    this.hllPlus = makeHllPlus();
  }

  @Override
  public void aggregate()
  {
    Object obj = selector.get();
    if (obj instanceof List) {
      for (Object o : (List) obj) {
        hllPlus.offer(o);
      }
    }
    if (obj instanceof HyperLogLogPlus) {
      try {
        hllPlus.addAll((HyperLogLogPlus) obj);
      } catch (CardinalityMergeException e) {
        throw Throwables.propagate(e);
      }
    } else {
      throw new UnsupportedOperationException(String.format("Unexpected object type[%s].", obj.getClass()));
    }
  }

  @Override
  public void reset()
  {
    hllPlus = makeHllPlus();
  }

  @Override
  public Object get()
  {
    return hllPlus;
  }

  @Override
  public float getFloat()
  {
    return hllPlus.cardinality();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {

  }
}
