package com.metamx.druid.query.search;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.Comparator;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LexicographicSearchSortSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "lexicographic", value = LexicographicSearchSortSpec.class),
    @JsonSubTypes.Type(name = "strlen", value = StrlenSearchSortSpec.class)
})
public interface SearchSortSpec
{
  public Comparator<SearchHit> getComparator();
}
