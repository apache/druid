package com.metamx.druid.index.brita;

import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

/**
 */
public class NotFilter implements Filter
{
  private final Filter baseFilter;

  public NotFilter(
      Filter baseFilter
  )
  {
    this.baseFilter = baseFilter;
  }

  @Override
  public ImmutableConciseSet goConcise(InvertedIndexSelector selector)
  {
    return ImmutableConciseSet.complement(
        baseFilter.goConcise(selector),
        selector.getNumRows()
    );
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    final ValueMatcher baseMatcher = baseFilter.makeMatcher(factory);

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return !baseMatcher.matches();
      }
    };
  }
}
