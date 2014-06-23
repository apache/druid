package io.druid.segment;

import com.google.common.collect.Iterators;
import io.druid.segment.data.IndexedInts;

import java.util.Iterator;

public class NullDimensionSelector implements DimensionSelector
{
  @Override
  public IndexedInts getRow()
  {
    return new IndexedInts()
    {
      @Override
      public int size()
      {
        return 1;
      }

      @Override
      public int get(int index)
      {
        return 0;
      }

      @Override
      public Iterator<Integer> iterator()
      {
        return Iterators.singletonIterator(0);
      }
    };
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
