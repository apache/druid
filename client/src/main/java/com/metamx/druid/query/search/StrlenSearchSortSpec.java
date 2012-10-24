package com.metamx.druid.query.search;

import java.util.Comparator;
import java.util.Map;

/**
 */
public class StrlenSearchSortSpec implements SearchSortSpec
{
  public StrlenSearchSortSpec()
  {
  }

  @Override
  public Comparator<SearchHit> getComparator()
  {
    return new Comparator<SearchHit>() {
      @Override
      public int compare(SearchHit s, SearchHit s1)
      {
        int res = s.getValue().length() - s1.getValue().length();
        if (res == 0) {
          return (s.getValue().compareTo(s1.getValue()));
        }
        return res;
      }
    };
  }

  public String toString()
  {
    return "stringLengthSort";
  }
}