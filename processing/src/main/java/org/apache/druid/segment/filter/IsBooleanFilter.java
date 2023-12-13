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
 * SQL three-value logic wrapper for some child {@link Filter} to implement '{filter} IS TRUE' and
 * '{filter} IS FALSE'. Primarily useful when living beneath a {@link NotFilter} because this filter purposely ignores
 * the value of {@code includeUnknown} and so always correctly only returns values that definitely match or do not match
 * the filter to produce correct results for '{filter} IS NOT TRUE' and '{filter} IS NOT FALSE'. This filter is a
 * relatively thin wrapper, so should be relatively harmless if used without a 'NOT' filter.
 *
 * @see org.apache.druid.query.filter.IsBooleanDimFilter
 * @see org.apache.druid.query.filter.IsTrueDimFilter
 * @see org.apache.druid.query.filter.IsFalseDimFilter
 */
public class IsBooleanFilter implements Filter
{
  private final Filter baseFilter;
  private final boolean isTrue;

  public IsBooleanFilter(Filter baseFilter, boolean isTrue)
  {
    this.baseFilter = baseFilter;
    this.isTrue = isTrue;
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    final BitmapColumnIndex baseIndex = baseFilter.getBitmapColumnIndex(selector);
    if (baseIndex != null && (isTrue || baseIndex.getIndexCapabilities().isInvertible())) {
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
          if (isTrue) {
            return baseIndex.computeBitmapResult(bitmapResultFactory, false);
          }
          return bitmapResultFactory.complement(
              baseIndex.computeBitmapResult(bitmapResultFactory, useThreeValueLogic),
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
        if (isTrue) {
          return baseMatcher.matches(false);
        }
        return !baseMatcher.matches(useThreeValueLogic);
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
        if (isTrue) {
          return baseMatcher.match(mask, false);
        }
        final ReadableVectorMatch baseMatch = baseMatcher.match(mask, useThreeValueLogic);

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
    return new IsBooleanFilter(baseFilter.rewriteRequiredColumns(columnRewrites), isTrue);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s) IS %s", baseFilter, isTrue ? "TRUE" : "FALSE");
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
    IsBooleanFilter isFilter = (IsBooleanFilter) o;
    return Objects.equals(baseFilter, isFilter.baseFilter);
  }

  @Override
  public int hashCode()
  {
    // to return a different hash from baseFilter
    return Objects.hash(1, baseFilter, isTrue);
  }
}
