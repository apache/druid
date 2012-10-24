package com.metamx.druid.collect;

import java.util.Iterator;

/**
*/
public class IteratorShell<T> implements Iterator<T>
{
  private final Iterator<T> base;

  IteratorShell(
      Iterator<T> base
  )
  {
    this.base = base;
  }

  @Override
  public boolean hasNext()
  {
    return base.hasNext();
  }

  @Override
  public T next()
  {
    return base.next();
  }

  @Override
  public void remove()
  {
    base.remove();
  }
}
