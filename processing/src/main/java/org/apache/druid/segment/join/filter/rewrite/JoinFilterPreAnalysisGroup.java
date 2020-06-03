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

import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;

import java.util.concurrent.ConcurrentHashMap;

public class JoinFilterPreAnalysisGroup
{
  private final boolean enableFilterPushDown;
  private final boolean enableFilterRewrite;
  private final boolean enableRewriteValueColumnFilters;
  private final long filterRewriteMaxSize;
  private final ConcurrentHashMap<Filter, JoinFilterPreAnalysis> analyses;

  public JoinFilterPreAnalysisGroup(
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
    this.analyses = new ConcurrentHashMap<>();
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

  public ConcurrentHashMap<Filter, JoinFilterPreAnalysis> getAnalyses()
  {
    return analyses;
  }
}
