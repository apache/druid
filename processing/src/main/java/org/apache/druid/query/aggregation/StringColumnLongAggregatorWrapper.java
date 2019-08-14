package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableValueLongColumnValueSelector;

import java.util.List;
import java.util.function.Function;

/**
 * This class can be used to wrap Long Aggregator that consume long type columns to handle String type.
 */
public class StringColumnLongAggregatorWrapper extends DelegatingAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final long nullValue;
  private final SettableValueLongColumnValueSelector longSelector;

  public StringColumnLongAggregatorWrapper(
      BaseObjectColumnValueSelector selector,
      Function<BaseLongColumnValueSelector, Aggregator> delegateBuilder,
      long nullValue
  )
  {
    this.longSelector = new SettableValueLongColumnValueSelector();
    this.selector = selector;
    this.nullValue = nullValue;
    this.delegate = delegateBuilder.apply(longSelector);
  }

  @Override
  public void aggregate()
  {
    Object update = selector.getObject();

    if (update == null) {
      longSelector.setValue(nullValue);
      delegate.aggregate();
    } else if (update instanceof List) {
      for (Object o : (List) update) {
        longSelector.setValue(Numbers.tryParseLong(o, nullValue));
        delegate.aggregate();
      }
    } else {
      longSelector.setValue(Numbers.tryParseLong(update, nullValue));
      delegate.aggregate();
    }
  }
}
