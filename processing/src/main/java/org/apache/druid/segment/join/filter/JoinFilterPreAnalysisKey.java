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
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * All the information that is required to generate a {@link JoinFilterPreAnalysis}.
 */
public class JoinFilterPreAnalysisKey
{
  private final JoinFilterRewriteConfig config;
  private final List<JoinableClause> clauses;
  private final VirtualColumns virtualColumns;

  @Nullable
  private final Filter filter;

  public JoinFilterPreAnalysisKey(
      final JoinFilterRewriteConfig config,
      final List<JoinableClause> clauses,
      final VirtualColumns virtualColumns,
      @Nullable final Filter filter
  )
  {
    this.config = config;
    this.clauses = clauses;
    this.virtualColumns = virtualColumns;
    this.filter = filter;
  }

  public JoinFilterRewriteConfig getRewriteConfig()
  {
    return config;
  }

  public List<JoinableClause> getJoinableClauses()
  {
    return clauses;
  }

  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  public Filter getFilter()
  {
    return filter;
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
    JoinFilterPreAnalysisKey that = (JoinFilterPreAnalysisKey) o;
    return Objects.equals(config, that.config) &&
           Objects.equals(clauses, that.clauses) &&
           Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(config, clauses, virtualColumns, filter);
  }
}
