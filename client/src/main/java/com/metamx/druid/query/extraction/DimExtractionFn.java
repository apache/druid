package com.metamx.druid.query.extraction;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property="type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "time", value = TimeDimExtractionFn.class),
    @JsonSubTypes.Type(name = "regex", value = RegexDimExtractionFn.class),
    @JsonSubTypes.Type(name = "partial", value = PartialDimExtractionFn.class),
    @JsonSubTypes.Type(name = "searchQuery", value = SearchQuerySpecDimExtractionFn.class)
})
public interface DimExtractionFn
{
  public byte[] getCacheKey();
  public String apply(String dimValue);
}
