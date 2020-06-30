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

package org.apache.druid.segment.join.filter.rewrite;

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;

import java.util.Objects;

/**
 * A config class that holds properties that control how join filter rewrites behave.
 */
public class JoinFilterRewriteConfig
{
  /**
   * Whether to enable filter push down optimizations to the base segment.
   * In production this should generally be {@code QueryContexts.getEnableJoinFilterPushDown(query)}.
   */
  private final boolean enableFilterPushDown;

  /**
   * Whether to enable filter rewrite optimizations for RHS columns.
   * In production this should generally be {@code QueryContexts.getEnableJoinFilterRewrite(query)}.
   */
  private final boolean enableFilterRewrite;

  /**
   * Whether to enable filter rewrite optimizations for RHS columns that are not key columns.
   * In production this should generally be {@code QueryContexts.getEnableJoinFilterRewriteValueColumnFilters(query)}.
   */
  private final boolean enableRewriteValueColumnFilters;

  /**
   * The max allowed size of correlated value sets for RHS rewrites. In production
   * This should generally be {@code QueryContexts.getJoinFilterRewriteMaxSize(query)}.
   */
  private final long filterRewriteMaxSize;

  public JoinFilterRewriteConfig(
      boolean enableFilterPushDown,
      boolean enableFilterRewrite,
      boolean enableRewriteValueColumnFilters,
      long filterRewriteMaxSize
  )
  {
    this.enableFilterPushDown = enableFilterPushDown;
    this.enableFilterRewrite = enableFilterRewrite;
    this.enableRewriteValueColumnFilters = enableRewriteValueColumnFilters;
    this.filterRewriteMaxSize = filterRewriteMaxSize;
  }

  public static JoinFilterRewriteConfig forQuery(final Query<?> query)
  {
    return new JoinFilterRewriteConfig(
        QueryContexts.getEnableJoinFilterPushDown(query),
        QueryContexts.getEnableJoinFilterRewrite(query),
        QueryContexts.getEnableJoinFilterRewriteValueColumnFilters(query),
        QueryContexts.getJoinFilterRewriteMaxSize(query)
    );
  }

  public boolean isEnableFilterPushDown()
  {
    return enableFilterPushDown;
  }

  public boolean isEnableFilterRewrite()
  {
    return enableFilterRewrite;
  }

  public boolean isEnableRewriteValueColumnFilters()
  {
    return enableRewriteValueColumnFilters;
  }

  public long getFilterRewriteMaxSize()
  {
    return filterRewriteMaxSize;
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
    JoinFilterRewriteConfig that = (JoinFilterRewriteConfig) o;
    return enableFilterPushDown == that.enableFilterPushDown &&
           enableFilterRewrite == that.enableFilterRewrite &&
           enableRewriteValueColumnFilters == that.enableRewriteValueColumnFilters &&
           filterRewriteMaxSize == that.filterRewriteMaxSize;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        enableFilterPushDown,
        enableFilterRewrite,
        enableRewriteValueColumnFilters,
        filterRewriteMaxSize
    );
  }
}
