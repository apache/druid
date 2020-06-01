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
import org.apache.druid.query.filter.vector.FalseVectorMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class FalseFilter implements Filter
{
  private static final FalseFilter INSTANCE = new FalseFilter();

  public static FalseFilter instance()
  {
    return INSTANCE;
  }

  private FalseFilter()
  {
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    return bitmapResultFactory.wrapAllFalse(Filters.allFalse(selector));
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    return 0;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return FalseValueMatcher.instance();
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    return new FalseVectorMatcher(factory.getVectorSizeInspector());
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return true;
  }

  @Override
  public boolean shouldUseBitmapIndex(BitmapIndexSelector selector)
  {
    return true;
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
  {
    return true;
  }

  @Override
  public boolean canVectorizeMatcher()
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return Collections.emptySet();
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    return this;
  }

  @Override
  public String toString()
  {
    return "false";
  }

  @Override
  public final int hashCode()
  {
    return FalseFilter.class.hashCode();
  }

  @Override
  public final boolean equals(Object obj)
  {
    return obj instanceof FalseFilter;
  }
}
