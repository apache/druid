package com.metamx.druid.query.filter;

import com.google.common.base.Joiner;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 */
public class OrDimFilter implements DimFilter
{
  private static final byte CACHE_TYPE_ID = 0x2;
  private static final Joiner OR_JOINER = Joiner.on(" || ");

  final private List<DimFilter> fields;

  @JsonCreator
  public OrDimFilter(
      @JsonProperty("fields") List<DimFilter> fields
  )
  {
    this.fields = fields;
  }

  @JsonProperty
  public List<DimFilter> getFields()
  {
    return fields;
  }

  @Override
  public byte[] getCacheKey()
  {
    return DimFilterCacheHelper.computeCacheKey(CACHE_TYPE_ID, fields);
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

    OrDimFilter that = (OrDimFilter) o;

    if (fields != null ? !fields.equals(that.fields) : that.fields != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return fields != null ? fields.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return String.format("(%s)", OR_JOINER.join(fields));
  }
}
