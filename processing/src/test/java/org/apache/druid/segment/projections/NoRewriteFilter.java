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

package org.apache.druid.segment.projections;

import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;

import java.util.Set;

/**
 * A test {@link Filter} that does not support required-column rewriting:
 * {@link Filter#supportsRequiredColumnRewrite()} is the default {@code false} and {@link Filter#rewriteRequiredColumns}
 * the default throwing implementation. Its index / matcher methods are unused by the projection-planning code under
 * test and throw if called.
 */
final class NoRewriteFilter implements Filter
{
  private final Set<String> requiredColumns;

  NoRewriteFilter(String... columns)
  {
    this.requiredColumns = Set.of(columns);
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return requiredColumns;
  }

  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException();
  }
}
