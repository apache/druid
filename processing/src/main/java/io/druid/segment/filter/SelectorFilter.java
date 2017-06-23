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

import io.druid.java.util.common.StringUtils;
import io.druid.query.BitmapResultFactory;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;

/**
 */
public class SelectorFilter implements Filter
{
  private final String dimension;
  private final String value;

  public SelectorFilter(
      String dimension,
      String value
  )
  {
    this.dimension = dimension;
    this.value = value;
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    return bitmapResultFactory.wrapDimensionValue(selector.getBitmapIndex(dimension, value));
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, dimension, value);
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }

  @Override
  public boolean supportsSelectivityEstimation(
      ColumnSelector columnSelector, BitmapIndexSelector indexSelector
  )
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    return (double) indexSelector.getBitmapIndex(dimension, value).size() / indexSelector.getNumRows();
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s = %s", dimension, value);
  }
}
