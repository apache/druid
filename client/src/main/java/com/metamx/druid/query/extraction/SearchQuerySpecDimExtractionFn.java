package com.metamx.druid.query.extraction;

import com.metamx.druid.query.search.SearchQuerySpec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;

/**
 */
public class SearchQuerySpecDimExtractionFn implements DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x3;

  private final SearchQuerySpec searchQuerySpec;

  @JsonCreator
  public SearchQuerySpecDimExtractionFn(
      @JsonProperty("query") SearchQuerySpec searchQuerySpec
  )
  {
    this.searchQuerySpec = searchQuerySpec;
  }

  @JsonProperty("query")
  public SearchQuerySpec getSearchQuerySpec()
  {
    return searchQuerySpec;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] specBytes = searchQuerySpec.getCacheKey();
    return ByteBuffer.allocate(1 + specBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(specBytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    return searchQuerySpec.accept(dimValue) ? dimValue : null;
  }

  @Override
  public String toString()
  {
    return "SearchQuerySpecDimExtractionFn{" +
           "searchQuerySpec=" + searchQuerySpec +
           '}';
  }
}
