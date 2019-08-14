package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableValueFloatColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

/**
 * This class can be used to wrap Float BufferAggregator that consume float type columns to handle String type.
 */
public class StringColumnFloatBufferAggregatorWrapper extends DelegatingBufferAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final float nullValue;
  private final SettableValueFloatColumnValueSelector floatSelector;

  public StringColumnFloatBufferAggregatorWrapper(
      BaseObjectColumnValueSelector selector,
      Function<BaseFloatColumnValueSelector, BufferAggregator> delegateBuilder,
      float nullValue
  )
  {
    this.floatSelector = new SettableValueFloatColumnValueSelector();
    this.selector = selector;
    this.nullValue = nullValue;
    this.delegate = delegateBuilder.apply(floatSelector);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object update = selector.getObject();

    if (update == null) {
      floatSelector.setValue(nullValue);
      delegate.aggregate(buf, position);
    } else if (update instanceof List) {
      for (Object o : (List) update) {
        floatSelector.setValue(Numbers.tryParseFloat(o, nullValue));
        delegate.aggregate(buf, position);
      }
    } else {
      floatSelector.setValue(Numbers.tryParseFloat(update, nullValue));
      delegate.aggregate(buf, position);
    }
  }
}
