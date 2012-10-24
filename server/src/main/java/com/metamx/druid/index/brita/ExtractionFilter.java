package com.metamx.druid.index.brita;

import com.google.common.collect.Lists;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.query.extraction.DimExtractionFn;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.util.List;

/**
 */
public class ExtractionFilter implements Filter
{
  private static final int MAX_SIZE = 50000;

  private final String dimension;
  private final String value;
  private final DimExtractionFn fn;

  public ExtractionFilter(
      String dimension,
      String value,
      DimExtractionFn fn
  )
  {
    this.dimension = dimension;
    this.value = value;
    this.fn = fn;
  }

  private List<Filter> makeFilters(InvertedIndexSelector selector)
  {
    final Indexed<String> allDimVals = selector.getDimensionValues(dimension);
    final List<Filter> filters = Lists.newArrayList();

    for (int i = 0; i < allDimVals.size(); i++) {
      String dimVal = allDimVals.get(i);
      if (value.equals(fn.apply(dimVal))) {
        filters.add(new SelectorFilter(dimension, dimVal));
      }
    }

    return filters;
  }

  @Override
  public ImmutableConciseSet goConcise(InvertedIndexSelector selector)
  {
    return new OrFilter(makeFilters(selector)).goConcise(selector);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    throw new UnsupportedOperationException();
  }
}
