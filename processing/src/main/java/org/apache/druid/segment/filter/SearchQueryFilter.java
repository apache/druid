/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.search.SearchQuerySpec;

import java.util.Map;
import java.util.Objects;

/**
 */
public class SearchQueryFilter extends DimensionPredicateFilter
{
  private final SearchQuerySpec query;

  @JsonCreator
  public SearchQueryFilter(
      @JsonProperty("dimension") final String dimension,
      @JsonProperty("query") final SearchQuerySpec query,
      @JsonProperty("extractionFn") final ExtractionFn extractionFn,
      @JsonProperty("filterTuning") final FilterTuning filterTuning
  )
  {
    super(
        dimension,
        new SearchQueryDruidPredicateFactory(query),
        extractionFn,
        filterTuning
    );

    this.query = query;
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(dimension);

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          dimension
      );
    }

    return new SearchQueryFilter(
        rewriteDimensionTo,
        query,
        extractionFn,
        filterTuning
    );
  }

  @Override
  public String toString()
  {
    return "SearchFilter{" +
           "query='" + query + '\'' +
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
    if (!super.equals(o)) {
      return false;
    }
    SearchQueryFilter that = (SearchQueryFilter) o;
    return Objects.equals(query, that.query);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), query);
  }

  @VisibleForTesting
  static class SearchQueryDruidPredicateFactory implements DruidPredicateFactory
  {
    private final SearchQuerySpec query;

    SearchQueryDruidPredicateFactory(SearchQuerySpec query)
    {
      this.query = query;
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate()
    {
      return input -> input == null ? DruidPredicateMatch.UNKNOWN : DruidPredicateMatch.of(query.accept(input));
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      return input -> DruidPredicateMatch.of(query.accept(String.valueOf(input)));
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      return input -> DruidPredicateMatch.of(query.accept(String.valueOf(input)));
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      return input -> DruidPredicateMatch.of(query.accept(String.valueOf(input)));
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
      SearchQueryDruidPredicateFactory that = (SearchQueryDruidPredicateFactory) o;
      return Objects.equals(query, that.query);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(query);
    }
  }
}
