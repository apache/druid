/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.filter.SearchQueryFilter;

import java.nio.ByteBuffer;

/**
 */
public class SearchQueryDimFilter implements DimFilter
{
  private final String dimension;
  private final SearchQuerySpec query;
  private final ExtractionFn extractionFn;

  public SearchQueryDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("query") SearchQuerySpec query,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(query != null, "query must not be null");

    this.dimension = dimension;
    this.query = query;
    this.extractionFn = extractionFn;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public SearchQuerySpec getQuery()
  {
    return query;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] queryBytes = query.getCacheKey();
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + queryBytes.length + extractionFnBytes.length)
                     .put(DimFilterUtils.SEARCH_QUERY_TYPE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(queryBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(extractionFnBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new SearchQueryFilter(dimension, query, extractionFn);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public String toString()
  {
    return "SearchQueryDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", query=" + query +
           ", extractionFn='" + extractionFn + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SearchQueryDimFilter)) {
      return false;
    }

    SearchQueryDimFilter that = (SearchQueryDimFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!query.equals(that.query)) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + query.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }
}
