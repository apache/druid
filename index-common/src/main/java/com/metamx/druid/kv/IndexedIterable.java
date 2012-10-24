package com.metamx.druid.kv;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class IndexedIterable<T> implements Iterable<T>
{
  private final Indexed<T> indexed;

  public static <T> IndexedIterable<T> create(Indexed<T> indexed)
  {
    return new IndexedIterable<T>(indexed);
  }

  public IndexedIterable(
    Indexed<T> indexed
  )
  {
    this.indexed = indexed;
  }

  @Override
  public Iterator<T> iterator()
  {
    return new Iterator<T>()
    {
      private int currIndex = 0;

      @Override
      public boolean hasNext()
      {
        return currIndex < indexed.size();
      }

      @Override
      public T next()
      {
        if (! hasNext()) {
          throw new NoSuchElementException();
        }
        return indexed.get(currIndex++);
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
