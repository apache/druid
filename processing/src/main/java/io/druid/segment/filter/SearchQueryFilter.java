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

package io.druid.segment.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DruidDoublePredicate;
import io.druid.query.filter.DruidFloatPredicate;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.search.search.SearchQuerySpec;

/**
 */
public class SearchQueryFilter extends DimensionPredicateFilter
{
  @JsonCreator
  public SearchQueryFilter(
      @JsonProperty("dimension") final String dimension,
      @JsonProperty("query") final SearchQuerySpec query,
      @JsonProperty("extractionFn") final ExtractionFn extractionFn
  )
  {
    super(
        dimension,
        new DruidPredicateFactory()
        {
          @Override
          public Predicate<String> makeStringPredicate()
          {
            return input -> query.accept(input);
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return input -> query.accept(String.valueOf(input));
          }

          @Override
          public DruidFloatPredicate makeFloatPredicate()
          {
            return input -> query.accept(String.valueOf(input));
          }

          @Override
          public DruidDoublePredicate makeDoublePredicate()
          {
            return input -> query.accept(String.valueOf(input));
          }
        },
        extractionFn
    );
  }
}
