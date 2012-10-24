package com.metamx.druid.query.filter;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type")
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="and", value=AndDimFilter.class),
    @JsonSubTypes.Type(name="or", value=OrDimFilter.class),
    @JsonSubTypes.Type(name="not", value=NotDimFilter.class),
    @JsonSubTypes.Type(name="selector", value=SelectorDimFilter.class),
    @JsonSubTypes.Type(name="extraction", value=ExtractionDimFilter.class),
    @JsonSubTypes.Type(name="regex", value=RegexDimFilter.class)
})
public interface DimFilter
{
  public byte[] getCacheKey();
}
