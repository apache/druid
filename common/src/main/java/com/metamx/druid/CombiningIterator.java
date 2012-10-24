package com.metamx.druid;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.metamx.common.guava.nary.BinaryFn;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class CombiningIterator<InType> implements Iterator<InType>
{
  public static <InType> CombiningIterator<InType> create(
      Iterator<InType> it,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, InType> fn
  )
  {
    return new CombiningIterator<InType>(it, comparator, fn);
  }

  private final PeekingIterator<InType> it;
  private final Comparator<InType> comparator;
  private final BinaryFn<InType, InType, InType> fn;

  public CombiningIterator(
      Iterator<InType> it,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, InType> fn
  )
  {
    this.it = Iterators.peekingIterator(it);
    this.comparator = comparator;
    this.fn = fn;
  }

  @Override
  public boolean hasNext()
  {
    return it.hasNext();
  }

  @Override
  public InType next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    InType res = null;

    while (hasNext()) {
      if (res == null) {
        res = fn.apply(it.next(), null);
        continue;
      }

      if (comparator.compare(res, it.peek()) == 0) {
        res = fn.apply(res, it.next());
      } else {
        break;
      }
    }

    return res;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
