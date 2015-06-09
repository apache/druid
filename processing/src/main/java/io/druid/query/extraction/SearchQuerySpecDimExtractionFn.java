/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.query.search.search.SearchQuerySpec;

import java.nio.ByteBuffer;

/**
 */
public class SearchQuerySpecDimExtractionFn extends DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x3;

  private final SearchQuerySpec searchQuerySpec;

  @JsonCreator
  public SearchQuerySpecDimExtractionFn(
      @JsonProperty("query") SearchQuerySpec searchQuerySpec
  )
  {
    Preconditions.checkNotNull(searchQuerySpec, "search query must not be null");

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
  public boolean preservesOrdering()
  {
    return true;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
  }

  @Override
  public String toString()
  {
    return "SearchQuerySpecDimExtractionFn{" +
           "searchQuerySpec=" + searchQuerySpec +
           '}';
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

    SearchQuerySpecDimExtractionFn that = (SearchQuerySpecDimExtractionFn) o;

    if (!searchQuerySpec.equals(that.searchQuerySpec)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return searchQuerySpec.hashCode();
  }
}
