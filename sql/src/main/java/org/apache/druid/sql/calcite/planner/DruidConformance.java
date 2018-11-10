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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.sql.validate.SqlAbstractConformance;

/**
 * Implementation of Calcite {@code SqlConformance} for Druid.
 */
public class DruidConformance extends SqlAbstractConformance
{
  private static final DruidConformance INSTANCE = new DruidConformance();

  private DruidConformance()
  {
    // Singleton.
  }

  public static DruidConformance instance()
  {
    return INSTANCE;
  }

  @Override
  public boolean isBangEqualAllowed()
  {
    // For x != y (as an alternative to x <> y)
    return true;
  }

  @Override
  public boolean isSortByOrdinal()
  {
    // For ORDER BY 1
    return true;
  }

  @Override
  public boolean isSortByAlias()
  {
    // For ORDER BY columnAlias (where columnAlias is a "column AS columnAlias")
    return true;
  }

  @Override
  public boolean isGroupByAlias()
  {
    // Disable GROUP BY columnAlias (where columnAlias is a "column AS columnAlias") since it causes ambiguity in the
    // Calcite validator for queries like SELECT TRIM(x) AS x, COUNT(*) FROM druid.foo GROUP BY TRIM(x) ORDER BY TRIM(x)
    return false;
  }

  @Override
  public boolean isGroupByOrdinal()
  {
    return true;
  }

  @Override
  public boolean isHavingAlias()
  {
    return true;
  }
}
