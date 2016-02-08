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
import com.metamx.common.IAE;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.topn.AlphaNumericTopNMetricSpec;
import io.druid.query.topn.LexicographicTopNMetricSpec;
import io.druid.segment.column.ValueType;

import java.util.Comparator;

public class BoundFilter extends DimensionPredicateFilter
{
  private static boolean applyString(final BoundDimFilter boundDimFilter, String input)
  {
    if (input == null) {
      return false;
    }

    Comparator<String> comparator;
    if (boundDimFilter.isAlphaNumeric()) {
      comparator = new AlphaNumericTopNMetricSpec(null).getComparator(null, null);
    } else {
      comparator = new LexicographicTopNMetricSpec(null).getComparator(null, null);
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

  private static Predicate getTypedPredicate(final BoundDimFilter boundDimFilter, ValueType type)
  {
    Predicate predicate = null;
    switch (type) {
      case STRING:
        predicate = new Predicate<String>()
        {
          @Override
          public boolean apply(String input)
          {
            return applyString(boundDimFilter, input);
          }
        };
        break;
      case LONG:
        final long parsedLowerLong;
        final long parsedUpperLong;
        try {
          parsedLowerLong = Long.parseLong(boundDimFilter.getLower());
          parsedUpperLong = Long.parseLong(boundDimFilter.getUpper());
        }
        catch (NumberFormatException nfe) {
          return null;
        }
        predicate = new Predicate<Long>()
        {
          @Override
          public boolean apply(Long input)
          {
            if (input == null) {
              return false;
            }
            if (input < parsedLowerLong || (boundDimFilter.isLowerStrict() && input == parsedLowerLong)) {
              return false;
            }
            if (input > parsedUpperLong || (boundDimFilter.isUpperStrict() && input == parsedUpperLong)) {
              return false;
            }
            return true;
          }
        };
        break;
      case FLOAT:
        final float parsedLowerFloat;
        final float parsedUpperFloat;
        try {
          parsedLowerFloat = Float.parseFloat(boundDimFilter.getLower());
          parsedUpperFloat = Float.parseFloat(boundDimFilter.getUpper());
        }
        catch (NumberFormatException nfe) {
          return null;
        }
        predicate = new Predicate<Float>()
        {
          @Override
          public boolean apply(Float input)
          {
            if (input == null) {
              return false;
            }
            if (input < parsedLowerFloat || (boundDimFilter.isLowerStrict() && input == parsedLowerFloat)) {
              return false;
            }
            if (input > parsedUpperFloat || (boundDimFilter.isUpperStrict() && input == parsedUpperFloat)) {
              return false;
            }
            return true;
          }
        };
        break;
      default:
        throw new IAE("Unsupported type for BoundFilter: " + type);
    }
    return predicate;
  }

  private static Predicate getPredicate(final BoundDimFilter boundDimFilter)
  {
    final Predicate stringPredicate = getTypedPredicate(boundDimFilter, ValueType.STRING);
    final Predicate longPredicate = getTypedPredicate(boundDimFilter, ValueType.LONG);
    final Predicate floatPredicate = getTypedPredicate(boundDimFilter, ValueType.FLOAT);

    Predicate predicate = new Predicate()
    {
      @Override
      public boolean apply(Object input)
      {
        if (input == null) {
          return false;
        }

        if (input instanceof String) {
          return stringPredicate.apply(input);
        }

        if (input instanceof Long) {
          if (longPredicate == null) {
            throw new IAE("Specified filter bounds are not compatible with Long column: %s,%s",
                          boundDimFilter.getLower(), boundDimFilter.getUpper()
            );
          }

          return longPredicate.apply(input);
        }

        if (input instanceof Float) {
          if (floatPredicate == null) {
            throw new IAE("Specified filter bounds are not compatible with Float column: %s,%s",
                          boundDimFilter.getLower(), boundDimFilter.getUpper()
            );
          }

          return floatPredicate.apply(input);
        }

        throw new IAE("Unsupported input type: " + input);
      }
    };

    return predicate;
  }

  public BoundFilter(final BoundDimFilter boundDimFilter)
  {
    super(boundDimFilter.getDimension(), getPredicate(boundDimFilter));
  }
}
