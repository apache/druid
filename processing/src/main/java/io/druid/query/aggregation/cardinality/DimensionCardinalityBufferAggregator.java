package io.druid.query.aggregation.cardinality;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.cardinality.hll.HyperLogLogPlus;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *
 */
public class DimensionCardinalityBufferAggregator implements BufferAggregator
{
  private static final byte[] initializingArray;

  static {
    HyperLogLogPlus empty = DimensionCardinalityAggregator.makeHllPlus();
    initializingArray = new byte[empty.sizeof()];
    empty.getBuffer().get(initializingArray);
  }

  private final ObjectColumnSelector selector;

  public DimensionCardinalityBufferAggregator(
      ObjectColumnSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer duplicate = buf.duplicate().order(buf.order());
    duplicate.position(position);
    duplicate.put(initializingArray);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    HyperLogLogPlus hll = (HyperLogLogPlus) get(buf, position);
    hll.offer(selector.get());
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer duplicate = buf.duplicate().order(ByteOrder.nativeOrder());
    duplicate.position(position);
    return new HyperLogLogPlus(duplicate);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return ((HyperLogLogPlus) get(buf, position)).cardinality();
  }

  @Override
  public void close()
  {
  }
}
