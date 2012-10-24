package com.metamx.druid.query.search;

import com.google.common.base.Predicate;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "insensitive_contains", value = InsensitiveContainsSearchQuerySpec.class),
    @JsonSubTypes.Type(name = "fragment", value = FragmentSearchQuerySpec.class)
})
public interface SearchQuerySpec
{
  public SearchSortSpec getSearchSortSpec();

  public boolean accept(String dimVal);

  public byte[] getCacheKey();
}
