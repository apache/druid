package org.apache.druid.query.aggregation.last;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.first.StringFirstLastUtils;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringLastVectorAggregator extends NumericLastVectorAggregator<String, SerializablePairLongString>
{
  // Initialized with MIN_VALUE instead of DateTimes.MIN.getMillis(), as it can be a custom timeSelector (provided via LATEST_BY)
  // that has a lower min than the times.min
  private static final SerializablePairLongString INIT = new SerializablePairLongString(
      Long.MIN_VALUE,
      null
  );

  private final int maxStringBytes;

  public StringLastVectorAggregator(
      @Nullable final VectorValueSelector timeSelector,
      final VectorObjectSelector objectSelector,
      final int maxStringBytes
  )
  {
    super(timeSelector, null, objectSelector);
    this.maxStringBytes = maxStringBytes;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    StringFirstLastUtils.writePair(buf, position, INIT, maxStringBytes);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return StringFirstLastUtils.readPair(buf, position);
  }

  @Override
  void putValue(ByteBuffer buf, int position, long time, String value)
  {
    StringFirstLastUtils.writePair(buf, position, new SerializablePairLongString(time, value), maxStringBytes);
  }

  @Override
  void putValue(ByteBuffer buf, int position, long time, VectorValueSelector valueSelector, int index)
  {
    throw DruidException.defensive("This variant is not applicable to the StringLastVectorAggregator");
  }

  @Override
  void putDefaultValue(ByteBuffer buf, int position, long time)
  {
    StringFirstLastUtils.writePair(
        buf,
        position,
        new SerializablePairLongString(time, NullHandling.defaultStringValue()),
        maxStringBytes
    );
  }

  @Override
  void putNull(ByteBuffer buf, int position, long time)
  {
    StringFirstLastUtils.writePair(buf, position, new SerializablePairLongString(time, null), maxStringBytes);
  }

  @Override
  SerializablePairLongString readPairFromVectorSelectors(
      boolean[] timeNullityVector,
      long[] timeVector,
      Object[] maybeFoldedObjects,
      int index
  )
  {
    return StringFirstLastUtils.readPairFromVectorSelectorsAtIndex(timeNullityVector, timeVector, maybeFoldedObjects, index);
  }
}
