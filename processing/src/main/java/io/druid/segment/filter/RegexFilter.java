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

import com.google.common.base.Predicate;
import io.druid.query.extraction.ExtractionFn;

import java.util.regex.Pattern;

/**
 */
public class RegexFilter extends DimensionPredicateFilter
{
  public RegexFilter(
      String dimension,
      final Pattern pattern,
      final ExtractionFn extractionFn
  )
  {
    super(
        dimension,
        new Predicate<String>()
        {
          @Override
          public boolean apply(String input)
          {
            return (input != null) && pattern.matcher(input).find();
          }
        },
        extractionFn
    );
  }
}
