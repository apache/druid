package com.metamx.druid.index.v1.processing;

/**
 * The "mutable" version of a ReadableOffset.  Introduces "increment()" and "withinBounds()" methods, which are
 * very similar to "next()" and "hasNext()" on the Iterator interface except increment() does not return a value.
 */
public interface Offset extends ReadableOffset
{
  void increment();

  boolean withinBounds();

  Offset clone();
}
