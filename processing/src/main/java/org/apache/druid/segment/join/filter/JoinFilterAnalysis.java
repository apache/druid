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

package org.apache.druid.segment.join.filter;

import org.apache.druid.query.filter.Filter;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Holds information about:
 * - whether a filter can be pushed down
 * - if it needs to be retained after the join,
 * - a reference to the original filter
 * - a potentially rewritten filter to be pushed down to the base table
 * - a list of virtual columns that need to be created on the base table to support the pushed down filter
 */
public class JoinFilterAnalysis
{
  private final boolean retainAfterJoin;
  private final Filter originalFilter;
  private final Optional<Filter> pushDownFilter;

  public JoinFilterAnalysis(
      boolean retainAfterJoin,
      Filter originalFilter,
      @Nullable Filter pushDownFilter
  )
  {
    this.retainAfterJoin = retainAfterJoin;
    this.originalFilter = originalFilter;
    this.pushDownFilter = pushDownFilter == null ? Optional.empty() : Optional.of(pushDownFilter);
  }

  public boolean isCanPushDown()
  {
    return pushDownFilter.isPresent();
  }

  public boolean isRetainAfterJoin()
  {
    return retainAfterJoin;
  }

  public Filter getOriginalFilter()
  {
    return originalFilter;
  }

  public Optional<Filter> getPushDownFilter()
  {
    return pushDownFilter;
  }

  /**
   * Utility method for generating an analysis that represents: "Filter cannot be pushed down"
   *
   * @param originalFilter The original filter which cannot be pushed down
   *
   * @return analysis that represents: "Filter cannot be pushed down"
   */
  public static JoinFilterAnalysis createNoPushdownFilterAnalysis(Filter originalFilter)
  {
    return new JoinFilterAnalysis(
        true,
        originalFilter,
        null
    );
  }
}
