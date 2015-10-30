package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;

/**
 */
public class ContainsSearchQuerySpec implements SearchQuerySpec
{

  private static final byte CACHE_TYPE_ID = 0x1;

  private final String value;
  private final boolean caseSensitive;

  private final String target;

  @JsonCreator
  public ContainsSearchQuerySpec(
      @JsonProperty("value") String value,
      @JsonProperty("caseSensitive") boolean caseSensitive
  )
  {
    this.value = value;
    this.caseSensitive = caseSensitive;
    this.target = value == null || caseSensitive ? value : value.toLowerCase();
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  public boolean isCaseSensitive()
  {
    return caseSensitive;
  }

  @Override
  public boolean accept(String dimVal)
  {
    if (dimVal == null) {
      return false;
    }
    final String input = caseSensitive ? dimVal : dimVal.toLowerCase();
    return input.contains(target);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] valueBytes = StringUtils.toUtf8(value);

    return ByteBuffer.allocate(2 + valueBytes.length)
                     .put(caseSensitive ? (byte)1 : 0)
                     .put(CACHE_TYPE_ID)
                     .put(valueBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "ContainsSearchQuerySpec{" +
           "value=" + value + ", caseSensitive=" + caseSensitive +
           "}";
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

    ContainsSearchQuerySpec that = (ContainsSearchQuerySpec) o;

    if (caseSensitive ^ that.caseSensitive) {
      return false;
    }

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
