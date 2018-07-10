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
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.topn.AlphaNumericTopNMetricSpec;
import io.druid.query.topn.LexicographicTopNMetricSpec;

import java.util.Comparator;

public class BoundFilter extends DimensionPredicateFilter
{

  public BoundFilter(final BoundDimFilter boundDimFilter)
  {
    super(
        boundDimFilter.getDimension(), new Predicate<String>()
        {
          private volatile Predicate<String> predicate;

          @Override
          public boolean apply(String input)
          {
            return function().apply(input);
          }

          private Predicate<String> function()
          {
            if (predicate == null) {
              final Comparator<String> comparator;
              if (boundDimFilter.isAlphaNumeric()) {
                comparator = new AlphaNumericTopNMetricSpec(null).getComparator(null, null);
              } else {
                comparator = new LexicographicTopNMetricSpec(null).getComparator(null, null);
              }
              predicate = new Predicate<String>()
              {
                @Override
                public boolean apply(String input)
                {
                  if (input == null) {
                    return false;
                  }
                  int lowerComparing = 1;
                  int upperComparing = 1;
                  if (boundDimFilter.getLower() != null) {
                    lowerComparing = comparator.compare(input, boundDimFilter.getLower());
                  }
                  if (boundDimFilter.getUpper() != null) {
                    upperComparing = comparator.compare(boundDimFilter.getUpper(), input);
                  }
                  if (boundDimFilter.isLowerStrict() && boundDimFilter.isUpperStrict()) {
                    return ((lowerComparing > 0)) && (upperComparing > 0);
                  } else if (boundDimFilter.isLowerStrict()) {
                    return (lowerComparing > 0) && (upperComparing >= 0);
                  } else if (boundDimFilter.isUpperStrict()) {
                    return (lowerComparing >= 0) && (upperComparing > 0);
                  }
                  return (lowerComparing >= 0) && (upperComparing >= 0);
                }
              };
            }
            return predicate;
          }
        }
    );
  }
}
