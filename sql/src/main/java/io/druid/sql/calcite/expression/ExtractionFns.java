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

package io.druid.sql.calcite.expression;

import com.google.common.collect.Lists;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.extraction.CascadeExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;

import java.util.Arrays;
import java.util.List;

public class ExtractionFns
{
  /**
   * Converts extractionFn to a QueryGranularity, if possible. This is the inverse of
   * {@link #fromQueryGranularity(Granularity)}.
   *
   * @param extractionFn function
   *
   * @return query granularity, or null if extractionFn cannot be translated
   */
  public static Granularity toQueryGranularity(final ExtractionFn extractionFn)
  {
    if (extractionFn instanceof TimeFormatExtractionFn) {
      final TimeFormatExtractionFn fn = (TimeFormatExtractionFn) extractionFn;
      if (fn.getFormat() == null && fn.getTimeZone() == null && fn.getLocale() == null && fn.isAsMillis()) {
        return fn.getGranularity();
      }
    }

    return null;
  }

  /**
   * Converts a QueryGranularity to an extractionFn, if possible. This is the inverse of
   * {@link #toQueryGranularity(ExtractionFn)}. This will always return a non-null extractionFn if
   * queryGranularity is non-null.
   *
   * @param queryGranularity granularity
   *
   * @return extractionFn, or null if queryGranularity is null
   */
  public static ExtractionFn fromQueryGranularity(final Granularity queryGranularity)
  {
    if (queryGranularity == null) {
      return null;
    } else {
      return new TimeFormatExtractionFn(null, null, null, queryGranularity, true);
    }
  }

  /**
   * Cascade f and g, returning an ExtractionFn that computes g(f(x)). Null f or g are treated like identity functions.
   *
   * @param f function
   * @param g function
   *
   * @return composed function, or null if both f and g were null
   */
  public static ExtractionFn cascade(final ExtractionFn f, final ExtractionFn g)
  {
    if (f == null) {
      // Treat null like identity.
      return g;
    } else if (g == null) {
      return f;
    } else {
      final List<ExtractionFn> extractionFns = Lists.newArrayList();

      // Apply g, then f, unwrapping if they are already cascades.

      if (f instanceof CascadeExtractionFn) {
        extractionFns.addAll(Arrays.asList(((CascadeExtractionFn) f).getExtractionFns()));
      } else {
        extractionFns.add(f);
      }

      if (g instanceof CascadeExtractionFn) {
        extractionFns.addAll(Arrays.asList(((CascadeExtractionFn) g).getExtractionFns()));
      } else {
        extractionFns.add(g);
      }

      return new CascadeExtractionFn(extractionFns.toArray(new ExtractionFn[extractionFns.size()]));
    }
  }
}
