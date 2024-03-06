package org.apache.druid.query.groupby.epinephelinae.column;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.ColumnValueSelector;

// Don't really use HolderType anywhere for now, we cast stuff everywhere, but perhaps with new selectors, we can
public interface MultiValueHelper<HolderType>
{
  Pair<HolderType, Integer> getMultiValueHolder(ColumnValueSelector selector, HolderType reusableValue);

  int multiValueSize(HolderType multiValueHolder);

  Pair<HolderType, Integer> getIndividualValueDictId(HolderType multiValueHolder, int index);
}
