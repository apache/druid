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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Provides facilities to create and re-use {@link VirtualColumn} definitions for dimensions, filters, and filtered
 * aggregators while constructing a {@link DruidQuery}.
 */
public class VirtualColumnRegistry
{
  private final RowSignature baseRowSignature;
  private final Map<ExpressionWrapper, VirtualColumn> virtualColumnsByExpression;
  private final Map<String, VirtualColumn> virtualColumnsByName;
  private final String virtualColumnPrefix;
  private int virtualColumnCounter;

  private VirtualColumnRegistry(
      RowSignature baseRowSignature,
      String virtualColumnPrefix,
      Map<ExpressionWrapper, VirtualColumn> virtualColumnsByExpression,
      Map<String, VirtualColumn> virtualColumnsByName
  )
  {
    this.baseRowSignature = baseRowSignature;
    this.virtualColumnPrefix = virtualColumnPrefix;
    this.virtualColumnsByExpression = virtualColumnsByExpression;
    this.virtualColumnsByName = virtualColumnsByName;
  }

  public static VirtualColumnRegistry create(final RowSignature rowSignature)
  {
    return new VirtualColumnRegistry(
        rowSignature,
        Calcites.findUnusedPrefixForDigits("v", rowSignature.getColumnNames()),
        new HashMap<>(),
        new HashMap<>()
    );
  }

  /**
   * Check if a {@link VirtualColumn} is defined by column name
   */
  public boolean isVirtualColumnDefined(String virtualColumnName)
  {
    return virtualColumnsByName.containsKey(virtualColumnName);
  }

  /**
   * Get existing or create new {@link VirtualColumn} for a given {@link DruidExpression} and {@link ValueType}.
   */
  public VirtualColumn getOrCreateVirtualColumnForExpression(
      PlannerContext plannerContext,
      DruidExpression expression,
      ValueType valueType
  )
  {
    ExpressionWrapper expressionWrapper = new ExpressionWrapper(expression.getExpression(), valueType);
    if (!virtualColumnsByExpression.containsKey(expressionWrapper)) {
      final String virtualColumnName = virtualColumnPrefix + virtualColumnCounter++;
      final VirtualColumn virtualColumn = expression.toVirtualColumn(
          virtualColumnName,
          valueType,
          plannerContext.getExprMacroTable()
      );
      virtualColumnsByExpression.put(
          expressionWrapper,
          virtualColumn
      );
      virtualColumnsByName.put(
          virtualColumnName,
          virtualColumn
      );
    }

    return virtualColumnsByExpression.get(expressionWrapper);
  }

  /**
   * Get existing or create new {@link VirtualColumn} for a given {@link DruidExpression} and {@link RelDataType}
   */
  public VirtualColumn getOrCreateVirtualColumnForExpression(
      PlannerContext plannerContext,
      DruidExpression expression,
      RelDataType dataType
  )
  {
    return getOrCreateVirtualColumnForExpression(
        plannerContext,
        expression,
        Calcites.getValueTypeForRelDataType(dataType)
    );
  }

  /**
   * Get existing virtual column by column name
   */
  @Nullable
  public VirtualColumn getVirtualColumn(String virtualColumnName)
  {
    return virtualColumnsByName.get(virtualColumnName);
  }

  @Nullable
  public VirtualColumn getVirtualColumnByExpression(String expression, RelDataType type)
  {
    ExpressionWrapper expressionWrapper = new ExpressionWrapper(expression, Calcites.getValueTypeForRelDataType(type));
    return virtualColumnsByExpression.get(expressionWrapper);
  }

  /**
   * Get a signature representing the base signature plus all registered virtual columns.
   */
  public RowSignature getFullRowSignature()
  {
    final RowSignature.Builder builder =
        RowSignature.builder().addAll(baseRowSignature);

    RowSignature baseSignature = builder.build();

    for (VirtualColumn virtualColumn : virtualColumnsByName.values()) {
      final String columnName = virtualColumn.getOutputName();
      builder.add(columnName, virtualColumn.capabilities(baseSignature, columnName).getType());
    }

    return builder.build();
  }

  /**
   * Given a list of column names, find any corresponding {@link VirtualColumn} with the same name
   */
  public List<VirtualColumn> findVirtualColumns(List<String> allColumns)
  {
    return allColumns.stream()
                     .filter(this::isVirtualColumnDefined)
                     .map(this::getVirtualColumn)
                     .collect(Collectors.toList());
  }

  private static class ExpressionWrapper
  {
    private final String expression;
    private final ValueType valueType;

    public ExpressionWrapper(String expression, ValueType valueType)
    {
      this.expression = expression;
      this.valueType = valueType;
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
      ExpressionWrapper expressionWrapper = (ExpressionWrapper) o;
      return Objects.equals(expression, expressionWrapper.expression) && valueType == expressionWrapper.valueType;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(expression, valueType);
    }
  }
}
