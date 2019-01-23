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

package org.apache.druid.sql.calcite.rel;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Wraps a {@link RowSignature} and provides facilities to re-use {@link VirtualColumn} definitions for dimensions,
 * filters, and filtered aggregators while constructing a {@link DruidQuery}
 */
public class DruidQuerySignature
{
  private final RowSignature rowSignature;

  private final Map<String, VirtualColumn> virtualColumnsByExpression;
  private final Map<String, VirtualColumn> virtualColumnsByName;
  private final String virtualColumnPrefix;
  private int virtualColumnCounter;

  private final boolean isImmutable;

  public DruidQuerySignature(RowSignature rowSignature)
  {
    this.isImmutable = false;
    this.rowSignature = rowSignature;
    this.virtualColumnPrefix = rowSignature == null ? "v" : Calcites.findUnusedPrefix(
        "v",
        new TreeSet<>(rowSignature.getRowOrder())
    );
    this.virtualColumnsByExpression = new HashMap<>();
    this.virtualColumnsByName = new HashMap<>();
  }

  private DruidQuerySignature(
      RowSignature rowSignature,
      String prefix,
      Map<String, VirtualColumn> virtualColumnsByExpression,
      Map<String, VirtualColumn> virtualColumnsByName,
      boolean isImmutable
  )
  {
    this.isImmutable = isImmutable;
    this.rowSignature = rowSignature;
    this.virtualColumnPrefix = prefix;
    this.virtualColumnsByExpression = virtualColumnsByExpression;
    this.virtualColumnsByName = virtualColumnsByName;
  }

  /**
   * Get {@link RowSignature} of {@link DruidQuery} under construction
   */
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  /**
   * Check if a {@link VirtualColumn} is defined by column name
   */
  public boolean isVirtualColumnDefined(String virtualColumnName)
  {
    return virtualColumnsByName.containsKey(virtualColumnName);
  }


  /**
   * Get existing or create new (if not {@link DruidQuerySignature#isImmutable}) {@link VirtualColumn} for a given
   * {@link DruidExpression}
   */
  @Nullable
  public VirtualColumn getOrCreateVirtualColumnForExpression(
      PlannerContext plannerContext,
      DruidExpression expression,
      SqlTypeName typeName
  )
  {
    if (!isImmutable && !virtualColumnsByExpression.containsKey(expression.getExpression())) {
      final String virtualColumnName = virtualColumnPrefix + virtualColumnCounter++;
      final VirtualColumn virtualColumn = expression.toVirtualColumn(
          virtualColumnName,
          Calcites.getValueTypeForSqlTypeName(typeName),
          plannerContext.getExprMacroTable()
      );
      virtualColumnsByExpression.put(
          expression.getExpression(),
          virtualColumn
      );
      virtualColumnsByName.put(
          virtualColumnName,
          virtualColumn
      );
    }

    VirtualColumn virtualColumn = virtualColumnsByExpression.get(expression.getExpression());

    if (virtualColumn == null) {
      return null;
    }
    return virtualColumn;
  }

  /**
   * Get existing virtual column by column name
   */
  @Nullable
  public VirtualColumn getVirtualColumn(String virtualColumnName)
  {
    return virtualColumnsByName.getOrDefault(virtualColumnName, null);
  }

  /**
   * Create a copy of existing state with new {@link RowSignature}, retaining virtual column definitions
   */
  public DruidQuerySignature withRowSignature(RowSignature signature)
  {
    return new DruidQuerySignature(
        signature,
        virtualColumnPrefix,
        virtualColumnsByExpression,
        virtualColumnsByName,
        isImmutable
    );
  }

  /**
   * Create an immutable copy of existing state, which allows re-use of predefined virtual columns, but doesn't allow
   * new definitions
   */
  public DruidQuerySignature asImmutable()
  {
    return new DruidQuerySignature(
        rowSignature,
        virtualColumnPrefix,
        ImmutableMap.copyOf(virtualColumnsByExpression),
        ImmutableMap.copyOf(virtualColumnsByName),
        true
    );
  }
}
