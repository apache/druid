package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableValueFloatColumnValueSelector;

import java.util.List;
import java.util.function.Function;

/**
 * This class can be used to wrap Float Aggregator that consume float type columns to handle String type.
 */
public class StringColumnFloatAggregatorWrapper extends DelegatingAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final float nullValue;
  private final SettableValueFloatColumnValueSelector floatSelector;

  public StringColumnFloatAggregatorWrapper(
      BaseObjectColumnValueSelector selector,
      Function<BaseFloatColumnValueSelector, Aggregator> delegateBuilder,
      float nullValue
  )
  {
    this.floatSelector = new SettableValueFloatColumnValueSelector();
    this.selector = selector;
    this.nullValue = nullValue;
    this.delegate = delegateBuilder.apply(floatSelector);
  }

  @Override
  public void aggregate()
  {
    Object update = selector.getObject();

    if (update == null) {
      floatSelector.setValue(nullValue);
      delegate.aggregate();
    } else if (update instanceof List) {
      for (Object o : (List) update) {
        floatSelector.setValue(Numbers.tryParseFloat(o, nullValue));
        delegate.aggregate();
      }
    } else {
      floatSelector.setValue(Numbers.tryParseFloat(update, nullValue));
      delegate.aggregate();
    }
  }
}
