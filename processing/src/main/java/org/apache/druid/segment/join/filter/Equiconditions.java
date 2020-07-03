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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.filter.Filter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

public class Equiconditions
{
  @Nonnull private final Map<String, Set<Expr>> equiconditions;

  public Equiconditions(Map<String, Set<Expr>> equiconditions)
  {
    this.equiconditions = equiconditions;
  }

  /**
   * @param filterClause the filter.
   * @return true if direct join filter rewrite is supported for the provided filter
   */
  public boolean doesFilterSupportDirectJoinFilterRewrite(Filter filterClause)
  {
    if (filterClause.supportsRequiredColumnRewrite()) {
      Set<String> requiredColumns = filterClause.getRequiredColumns();
      if (requiredColumns.size() == 1) {
        String reqColumn = requiredColumns.iterator().next();
        return equiconditions.containsKey(reqColumn);
      }
    }
    return false;
  }

  @Nullable
  public Set<Expr> getLhsExprs(String rhsColumn)
  {
    return equiconditions.get(rhsColumn);
  }
}
