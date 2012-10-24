package com.metamx.druid.index.brita;

import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;


/**
 */
public class SelectorFilter implements Filter
{
  private final String dimension;
  private final String value;

  public SelectorFilter(
      String dimension,
      String value
  )
  {
    this.dimension = dimension;
    this.value = value;
  }

  @Override
  public ImmutableConciseSet goConcise(InvertedIndexSelector selector)
  {
    return selector.getConciseInvertedIndex(dimension, value);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, value);
  }
}
