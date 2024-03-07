package org.apache.druid.query.groupby.epinephelinae.column;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.ColumnValueSelector;

// Don't really use DimensionHolderType anywhere for now, we cast stuff everywhere, but perhaps with new selectors, we can
public interface KeyToId<DimensionHolderType>
{
  Pair<DimensionHolderType, Integer> getMultiValueHolder(ColumnValueSelector selector, DimensionHolderType reusableValue);

  int multiValueSize(DimensionHolderType multiValueHolder);

  Pair<Integer, Integer> getIndividualValueDictId(DimensionHolderType multiValueHolder, int index);
}
