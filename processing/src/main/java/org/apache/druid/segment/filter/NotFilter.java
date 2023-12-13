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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BaseVectorValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Nice filter you have there... NOT!
 *
 * If {@link ExpressionProcessing#useStrictBooleans()} and {@link NullHandling#sqlCompatible()} are both true, this
 * filter inverts the {@code includeUnknown} flag to properly map Druids native two-valued logic (true, false) to SQL
 * three-valued logic (true, false, unknown). At the top level, this flag is always passed in as 'false', and is only
 * flipped by this filter. Other logical filters ({@link AndFilter} and {@link OrFilter}) propagate the value of
 * {@code includeUnknown} to their children.
 *
 * For example, if the base filter is equality, by default value matchers and indexes only return true for the rows
 * that are equal to the value. When wrapped in a not filter, the not filter indicates that the equality matchers and
 * indexes should also include the null or 'unknown' values as matches, so that inverting the match does not incorrectly
 * include these null values as matches.
 */
public class NotFilter implements Filter
{
  private final Filter baseFilter;

  public NotFilter(Filter baseFilter)
  {
    this.baseFilter = baseFilter;
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    final BitmapColumnIndex baseIndex = baseFilter.getBitmapColumnIndex(selector);
    if (baseIndex != null && baseIndex.getIndexCapabilities().isInvertible()) {
      return new BitmapColumnIndex()
      {
        private final boolean useThreeValueLogic = NullHandling.useThreeValueLogic();
        @Override
        public ColumnIndexCapabilities getIndexCapabilities()
        {
          return baseIndex.getIndexCapabilities();
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          return bitmapResultFactory.complement(
              baseIndex.computeBitmapResult(bitmapResultFactory, !includeUnknown && useThreeValueLogic),
              selector.getNumRows()
          );
        }
      };
    }
    return null;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final ValueMatcher baseMatcher = baseFilter.makeMatcher(factory);

    return new ValueMatcher()
    {
      private final boolean useThreeValueLogic = NullHandling.useThreeValueLogic();
      @Override
      public boolean matches(boolean includeUnknown)
      {
        return !baseMatcher.matches(!includeUnknown && useThreeValueLogic);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("baseMatcher", baseMatcher);
      }
    };
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    final VectorValueMatcher baseMatcher = baseFilter.makeVectorMatcher(factory);

    return new BaseVectorValueMatcher(baseMatcher)
    {
      private final VectorMatch scratch = VectorMatch.wrap(new int[factory.getMaxVectorSize()]);
      private final boolean useThreeValueLogic = NullHandling.useThreeValueLogic();

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        final ReadableVectorMatch baseMatch = baseMatcher.match(mask, !includeUnknown && useThreeValueLogic);

        scratch.copyFrom(mask);
        scratch.removeAll(baseMatch);
        assert scratch.isValid(mask);
        return scratch;
      }
    };
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return baseFilter.canVectorizeMatcher(inspector);
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return baseFilter.getRequiredColumns();
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return baseFilter.supportsRequiredColumnRewrite();
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    return new NotFilter(baseFilter.rewriteRequiredColumns(columnRewrites));
  }

  @Override
  public String toString()
  {
    return StringUtils.format("~(%s)", baseFilter);
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
    NotFilter notFilter = (NotFilter) o;
    return Objects.equals(baseFilter, notFilter.baseFilter);
  }

  @Override
  public int hashCode()
  {
    // to return a different hash from baseFilter
    return Objects.hash(1, baseFilter);
  }

  public Filter getBaseFilter()
  {
    return baseFilter;
  }
}
