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

package org.apache.druid.segment;

import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;

/**
 * It likely looks weird that we are creating a new instance of ColumnSelector here that begins its life deprecated
 * and only delegates methods to the Queryable Index. This is done intentionally so that the QueryableIndex doesn't
 * accidentally get used as a ColumnSelector.
 *
 * The lifecycle of the QueryableIndex is over the lifetime of the segment on a specific process, while
 * the ColumnSelector's lifecycle is for a given query.  When we don't use the same ColumnSelector for an
 * entire query, we defeat caching and use a lot more resources than necessary for queries.
 *
 * Places that use this class are intentionally circumventing column caching and column lifecycle management,
 * ostensibly because those code locations know that they are only looking at metadata.  If a code path uses this
 * and actually accesses a column instead of just looking at metadata, it will leak any resources that said column
 * requires.
 *
 * The ColumnCache is the preferred implementation of a ColumnSelector, it takes a Closer and that closer can be used
 * to ensure that resources are cleaned up.
 */
@Deprecated
public class DeprecatedQueryableIndexColumnSelector implements ColumnSelector
{
  private final QueryableIndex index;

  public DeprecatedQueryableIndexColumnSelector(QueryableIndex index)
  {
    this.index = index;
  }

  @Nullable
  @Override
  public ColumnHolder getColumnHolder(String columnName)
  {
    return index.getColumnHolder(columnName);
  }
}
