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

import com.google.common.base.Predicate;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.FilterTuning;

import java.util.regex.Pattern;

/**
 */
public class RegexFilter extends DimensionPredicateFilter
{
  public RegexFilter(
      final String dimension,
      final Pattern pattern,
      final ExtractionFn extractionFn,
      final FilterTuning filterTuning
  )
  {
    super(
        dimension,
        new DruidPredicateFactory()
        {
          @Override
          public Predicate<String> makeStringPredicate()
          {
            return input -> (input != null) && pattern.matcher(input).find();
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return input -> pattern.matcher(String.valueOf(input)).find();
          }

          @Override
          public DruidFloatPredicate makeFloatPredicate()
          {
            return input -> pattern.matcher(String.valueOf(input)).find();
          }

          @Override
          public DruidDoublePredicate makeDoublePredicate()
          {
            return input -> pattern.matcher(String.valueOf(input)).find();
          }

          @Override
          public String toString()
          {
            return "RegexFilter{" +
                   "pattern='" + pattern + '\'' +
                   '}';
          }
        },
        extractionFn,
        filterTuning
    );
  }
}
