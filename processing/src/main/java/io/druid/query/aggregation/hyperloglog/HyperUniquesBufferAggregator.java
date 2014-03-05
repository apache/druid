package io.druid.query.aggregation.hyperloglog;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class HyperUniquesBufferAggregator implements BufferAggregator
{
  private static final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();
  private final ObjectColumnSelector selector;

  public HyperUniquesBufferAggregator(
      ObjectColumnSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.put(EMPTY_BYTES);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    HyperLogLogCollector collector = (HyperLogLogCollector) selector.get();

    if (collector == null) {
      return;
    }

    HyperLogLogCollector.makeCollector(
        (ByteBuffer) buf.duplicate().position(position).limit(
            position
            + HyperLogLogCollector.getLatestNumBytesForDenseStorage()
        )
    ).fold(collector);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer dataCopyBuffer = ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage());
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.get(dataCopyBuffer.array());
    return HyperLogLogCollector.makeCollector(dataCopyBuffer);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
