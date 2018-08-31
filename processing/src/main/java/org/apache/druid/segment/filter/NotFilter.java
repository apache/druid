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

import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;

/**
 */
public class NotFilter implements Filter
{
  private final Filter baseFilter;

  public NotFilter(
      Filter baseFilter
  )
  {
    this.baseFilter = baseFilter;
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    return bitmapResultFactory.complement(
        baseFilter.getBitmapResult(selector, bitmapResultFactory),
        selector.getNumRows()
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final ValueMatcher baseMatcher = baseFilter.makeMatcher(factory);

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return !baseMatcher.matches();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("baseMatcher", baseMatcher);
      }
    };
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return baseFilter.supportsBitmapIndex(selector);
  }

  @Override
  public boolean supportsSelectivityEstimation(
      ColumnSelector columnSelector, BitmapIndexSelector indexSelector
  )
  {
    return baseFilter.supportsSelectivityEstimation(columnSelector, indexSelector);
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    return 1. - baseFilter.estimateSelectivity(indexSelector);
  }

  public Filter getBaseFilter()
  {
    return baseFilter;
  }
}
