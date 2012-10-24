package com.metamx.druid.query.search;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;

/**
 */
public class InsensitiveContainsSearchQuerySpec implements SearchQuerySpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final String value;
  private final SearchSortSpec sortSpec;

  @JsonCreator
  public InsensitiveContainsSearchQuerySpec(
      @JsonProperty("value") String value,
      @JsonProperty("sort") SearchSortSpec sortSpec
  )
  {
    this.value = value.toLowerCase();
    this.sortSpec = (sortSpec == null) ? new LexicographicSearchSortSpec() : sortSpec;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty("sort")
  @Override
  public SearchSortSpec getSearchSortSpec()
  {
    return sortSpec;
  }

  @Override
  public boolean accept(String dimVal)
  {
    return dimVal.toLowerCase().contains(value);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] valueBytes = value.getBytes();

    return ByteBuffer.allocate(1 + valueBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(valueBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "InsensitiveContainsSearchQuerySpec{" +
             "value=" + value +
             ", sortSpec=" + sortSpec +
           "}";
  }
}
