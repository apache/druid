package io.druid.query.aggregation.cardinality;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Throwables;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public class DimensionCardinalityBufferAggregator implements BufferAggregator
{
  private static final byte[] emptyBytes;

  static {
    try {
      emptyBytes = DimensionCardinalityAggregator.makeHllPlus().getBytes();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
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
    ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    duplicate.putInt(emptyBytes.length);
    duplicate.put(emptyBytes);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    HyperLogLogPlus hll = (HyperLogLogPlus) get(buf, position);

    Object obj = selector.get();
    if (obj == null) {
      hll.offer(obj);
    }
    else if (obj instanceof List) {
      for (Object o : (List) obj) {
        hll.offer(o);
      }
    }
    else if (obj instanceof HyperLogLogPlus) {
      try {
        hll.addAll((HyperLogLogPlus) obj);
      } catch (CardinalityMergeException e) {
        throw Throwables.propagate(e);
      }
    } else if (obj instanceof String) {
      hll.offer(obj);
    } else {
      throw new UnsupportedOperationException(String.format("Unexpected object type[%s].", obj.getClass()));
    }

    writeHll(buf, position, hll);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    int size = duplicate.getInt();

    byte[] bytes = new byte[size];
    duplicate.get(bytes);
    return DimensionCardinalityAggregator.fromBytes(bytes);
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

  private void writeHll(ByteBuffer buf, int position, HyperLogLogPlus hll)
  {
    try {
      byte[] outBytes = hll.getBytes();
      ByteBuffer outBuf = buf.duplicate();
      outBuf.position(position);
      outBuf.putInt(outBytes.length);
      outBuf.put(outBytes);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
