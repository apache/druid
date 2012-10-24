package com.metamx.druid.result;

import com.metamx.druid.query.search.SearchHit;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import java.util.Iterator;
import java.util.List;

/**
 */
public class SearchResultValue implements Iterable<SearchHit>
{
  private final List<SearchHit> value;

  @JsonCreator
  public SearchResultValue(
      List<SearchHit> value
  )
  {
    this.value = value;
  }

  @JsonValue
  public List<SearchHit> getValue()
  {
    return value;
  }

  @Override
  public Iterator<SearchHit> iterator()
  {
    return value.iterator();
  }

  @Override
  public String toString()
  {
    return "SearchResultValue{" +
           "value=" + value +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SearchResultValue that = (SearchResultValue) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }
}
