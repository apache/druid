package com.metamx.druid.index.brita;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.metamx.common.guava.FunctionalIterable;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import javax.annotation.Nullable;

/**
 */
class DimensionPredicateFilter implements Filter
{
  private final String dimension;
  private final Predicate<String> predicate;

  public DimensionPredicateFilter(
      String dimension,
      Predicate<String> predicate
  )
  {
    this.dimension = dimension;
    this.predicate = predicate;
  }

  @Override
  public ImmutableConciseSet goConcise(final InvertedIndexSelector selector)
  {
    return ImmutableConciseSet.union(
        FunctionalIterable.create(selector.getDimensionValues(dimension))
                          .filter(predicate)
                          .transform(
                              new Function<String, ImmutableConciseSet>()
                              {
                                @Override
                                public ImmutableConciseSet apply(@Nullable String input)
                                {
                                  return selector.getConciseInvertedIndex(dimension, input);
                                }
                              }
                          )
    );
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, predicate);
  }
}
