package io.druid.segment;

import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.SingleIndexedInts;

public class NullDimensionSelector implements DimensionSelector
{
  @Override
  public IndexedInts getRow()
  {
    return new SingleIndexedInts(0);
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Override
  public String lookupName(int id)
  {
    return null;
  }

  @Override
  public int lookupId(String name)
  {
    return 0;
  }
}
