package com.metamx.druid.query.filter;

import com.google.common.base.Charsets;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;

/**
 */
public class RegexDimFilter implements DimFilter
{
  private static final byte CACHE_ID_KEY = 0x5;
  private final String dimension;
  private final String pattern;

  @JsonCreator
  public RegexDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("pattern") String pattern
  )
  {
    this.dimension = dimension;
    this.pattern = pattern;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = dimension.getBytes(Charsets.UTF_8);
    final byte[] patternBytes = pattern.getBytes(Charsets.UTF_8);

    return ByteBuffer.allocate(1 + dimensionBytes.length + patternBytes.length)
        .put(CACHE_ID_KEY)
        .put(dimensionBytes)
        .put(patternBytes)
        .array();
  }
}
