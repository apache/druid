package com.metamx.druid.index.brita;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.druid.query.filter.AndDimFilter;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.query.filter.ExtractionDimFilter;
import com.metamx.druid.query.filter.NotDimFilter;
import com.metamx.druid.query.filter.OrDimFilter;
import com.metamx.druid.query.filter.RegexDimFilter;
import com.metamx.druid.query.filter.SelectorDimFilter;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class Filters
{
  public static List<Filter> convertDimensionFilters(List<DimFilter> filters){
    return Lists.transform(
        filters,
        new Function<DimFilter, Filter>()
        {
          @Override
          public Filter apply(@Nullable DimFilter input)
          {
            return convertDimensionFilters(input);
          }
        }
    );
  }

  public static Filter convertDimensionFilters(DimFilter dimFilter)
  {
    if (dimFilter == null) {
      return null;
    }

    Filter filter = null;
    if (dimFilter instanceof AndDimFilter) {
      filter = new AndFilter(convertDimensionFilters(((AndDimFilter) dimFilter).getFields()));
    } else if (dimFilter instanceof OrDimFilter) {
      filter = new OrFilter(convertDimensionFilters(((OrDimFilter) dimFilter).getFields()));
    } else if (dimFilter instanceof NotDimFilter) {
      filter = new NotFilter(convertDimensionFilters(((NotDimFilter) dimFilter).getField()));
    } else if (dimFilter instanceof SelectorDimFilter) {
      final SelectorDimFilter selectorDimFilter = (SelectorDimFilter) dimFilter;

      filter = new SelectorFilter(selectorDimFilter.getDimension(), selectorDimFilter.getValue());
    } else if (dimFilter instanceof ExtractionDimFilter) {
      final ExtractionDimFilter extractionDimFilter = (ExtractionDimFilter) dimFilter;

      filter = new ExtractionFilter(
          extractionDimFilter.getDimension(),
          extractionDimFilter.getValue(),
          extractionDimFilter.getDimExtractionFn()
      );
    } else if (dimFilter instanceof RegexDimFilter) {
      final RegexDimFilter regexDimFilter = (RegexDimFilter) dimFilter;

      filter = new RegexFilter(regexDimFilter.getDimension(), regexDimFilter.getPattern());
    }

    return filter;
  }
}
