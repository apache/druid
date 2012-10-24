package com.metamx.druid.index.brita;

import com.google.common.collect.Lists;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.util.List;

/**
 */
public class OrFilter implements Filter
{
  private final List<Filter> filters;

  public OrFilter(
      List<Filter> filters
  )
  {
    if (filters.size() == 0) {
      throw new IllegalArgumentException("Can't construct empty OrFilter (the universe does not exist)");
    }

    this.filters = filters;
  }

  @Override
  public ImmutableConciseSet goConcise(InvertedIndexSelector selector)
  {
    if (filters.size() == 1) {
      return filters.get(0).goConcise(selector);
    }

    List<ImmutableConciseSet> conciseSets = Lists.newArrayList();
    for (int i = 0; i < filters.size(); i++) {
      conciseSets.add(filters.get(i).goConcise(selector));
    }

    return ImmutableConciseSet.union(conciseSets);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    final ValueMatcher[] matchers = new ValueMatcher[filters.size()];

    for (int i = 0; i < filters.size(); i++) {
      matchers[i] = filters.get(i).makeMatcher(factory);
    }

    if (matchers.length == 1) {
      return matchers[0];
    }

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        for (ValueMatcher matcher : matchers) {
          if (matcher.matches()) {
            return true;
          }
        }
        return false;
      }
    };
  }
}
