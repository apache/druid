package com.metamx.druid.query.search;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 */
public class SearchHit implements Comparable<SearchHit>
{
  private final String dimension;
  private final String value;

  @JsonCreator
  public SearchHit(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value
  )
  {
    this.dimension = checkNotNull(dimension);
    this.value = checkNotNull(value);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Override
  public int compareTo(SearchHit o)
  {
    int retVal = dimension.compareTo(o.dimension);
    if (retVal == 0) {
      retVal = value.compareTo(o.value);
    }
    return retVal;
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

    SearchHit searchHit = (SearchHit) o;

    if (dimension != null ? !dimension.equals(searchHit.dimension) : searchHit.dimension != null) {
      return false;
    }
    if (value != null ? !value.equals(searchHit.value) : searchHit.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Hit{" +
           "dimension='" + dimension + '\'' +
           ", value='" + value + '\'' +
           '}';
  }
}
