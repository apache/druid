package com.metamx.druid.query.search;

import org.codehaus.jackson.annotate.JsonCreator;

import java.util.Comparator;
import java.util.Map;

/**
 */
public class LexicographicSearchSortSpec implements SearchSortSpec
{
  @JsonCreator
  public LexicographicSearchSortSpec(
  )
  {
  }

  @Override
  public Comparator<SearchHit> getComparator()
  {
    return new Comparator<SearchHit>()
    {
      @Override
      public int compare(SearchHit searchHit, SearchHit searchHit1)
      {
        return searchHit.getValue().compareTo(searchHit1.getValue());
      }
    };
  }

  public String toString()
  {
    return "lexicographicSort";
  }
}
