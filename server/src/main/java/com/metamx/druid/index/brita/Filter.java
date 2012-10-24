package com.metamx.druid.index.brita;

import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

/**
 */
public interface Filter
{
  public ImmutableConciseSet goConcise(InvertedIndexSelector selector);
  public ValueMatcher makeMatcher(ValueMatcherFactory factory);
}
