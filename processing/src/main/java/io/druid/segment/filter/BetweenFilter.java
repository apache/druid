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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.guava.FunctionalIterable;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.data.Indexed;
import org.apache.commons.lang.math.NumberUtils;

import javax.annotation.Nullable;

/**
 * Created by zhxiaog on 15/11/12.
 */
public class BetweenFilter implements Filter
{

  private final String dimension;
  private final Predicate<String> predicate;

  public BetweenFilter(String dimension, boolean numrically, Object lower, Object upper)
  {
    this.dimension = dimension;
    if (numrically) {
      this.predicate = new FloatBoundPredicate((float) lower, (float) upper);
    } else {
      this.predicate = new StringBoundPredicate((String) lower, (String) upper);
    }
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    Indexed<String> dimValues = selector.getDimensionValues(this.dimension);
    ImmutableBitmap result = null;
    if (dimValues == null) {
      result = selector.getBitmapFactory().makeEmptyImmutableBitmap();
    } else {
      result = selector.getBitmapFactory().union(
          FunctionalIterable.create(dimValues)
                            .filter(predicate)
                            .transform(
                                new Function<String, ImmutableBitmap>()
                                {
                                  @Nullable
                                  @Override
                                  public ImmutableBitmap apply(String input)
                                  {
                                    return selector.getBitmapIndex(
                                        BetweenFilter.this.dimension,
                                        input
                                    );
                                  }
                                }
                            )

      );
    }
    return result;
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(this.dimension, this.predicate);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * between operator for float values
   */
  private static class FloatBoundPredicate implements com.google.common.base.Predicate<String>
  {
    private float lower;
    private float upper;

    public FloatBoundPredicate(float lower, float upper)
    {
      this.lower = lower;
      this.upper = upper;
    }

    @Override
    public boolean apply(@Nullable String input)
    {
      if (NumberUtils.isNumber(input)) {
        float num = NumberUtils.toFloat(input);
        return Float.compare(num, this.upper) <= 0 && Float.compare(num, this.lower) >= 0;
      } else {
        return false;
      }
    }
  }

  /**
   * between operator for string values
   */
  private static class StringBoundPredicate implements com.google.common.base.Predicate<String>
  {
    private String lower;
    private String upper;

    public StringBoundPredicate(String lower, String upper)
    {
      this.lower = lower;
      this.upper = upper;
    }

    @Override
    public boolean apply(@Nullable String input)
    {
      return input != null && input.compareTo(this.upper) <= 0 && input.compareTo(this.lower) >= 0;
    }
  }


}
