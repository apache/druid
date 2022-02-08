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
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Provides facilities to create and re-use {@link VirtualColumn} definitions for dimensions, filters, and filtered
 * aggregators while constructing a {@link DruidQuery}.
 */
public class VirtualColumnRegistry
{
  private final ExprMacroTable macroTable;
  private final RowSignature baseRowSignature;
  private final Map<ExpressionAndTypeHint, String> virtualColumnsByExpression;
  private final Map<String, ExpressionAndTypeHint> virtualColumnsByName;
  private final String virtualColumnPrefix;
  private int virtualColumnCounter;

  private VirtualColumnRegistry(
      RowSignature baseRowSignature,
      ExprMacroTable macroTable,
      String virtualColumnPrefix,
      Map<ExpressionAndTypeHint, String> virtualColumnsByExpression,
      Map<String, ExpressionAndTypeHint> virtualColumnsByName
  )
  {
    this.macroTable = macroTable;
    this.baseRowSignature = baseRowSignature;
    this.virtualColumnPrefix = virtualColumnPrefix;
    this.virtualColumnsByExpression = virtualColumnsByExpression;
    this.virtualColumnsByName = virtualColumnsByName;
  }

  public static VirtualColumnRegistry create(final RowSignature rowSignature, final ExprMacroTable macroTable)
  {
    return new VirtualColumnRegistry(
        rowSignature,
        macroTable,
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
   * Get existing or create new {@link VirtualColumn} for a given {@link DruidExpression} and hinted {@link ColumnType}.
   */
  public String getOrCreateVirtualColumnForExpression(
      DruidExpression expression,
      ColumnType typeHint
  )
  {
    final ExpressionAndTypeHint candidate = wrap(expression, typeHint);
    if (!virtualColumnsByExpression.containsKey(candidate)) {
      final String virtualColumnName = virtualColumnPrefix + virtualColumnCounter++;

      virtualColumnsByExpression.put(
          candidate,
          virtualColumnName
      );
      virtualColumnsByName.put(
          virtualColumnName,
          candidate
      );
    }

    return virtualColumnsByExpression.get(candidate);
  }

  /**
   * Get existing or create new {@link VirtualColumn} for a given {@link DruidExpression} and {@link RelDataType}
   */
  public String getOrCreateVirtualColumnForExpression(
      DruidExpression expression,
      RelDataType typeHint
  )
  {
    return getOrCreateVirtualColumnForExpression(
        expression,
        Calcites.getColumnTypeForRelDataType(typeHint)
    );
  }

  /**
   * Get existing virtual column by column name
   */
  @Nullable
  public VirtualColumn getVirtualColumn(String virtualColumnName)
  {
    return Optional.ofNullable(virtualColumnsByName.get(virtualColumnName))
                   .map(v -> v.getExpression().toVirtualColumn(virtualColumnName, v.getTypeHint(), macroTable))
                   .orElse(null);
  }

  @Nullable
  public String getVirtualColumnByExpression(DruidExpression expression, RelDataType typeHint)
  {
    return virtualColumnsByExpression.get(wrap(expression, Calcites.getColumnTypeForRelDataType(typeHint)));
  }

  /**
   * Get a signature representing the base signature plus all registered virtual columns.
   */
  public RowSignature getFullRowSignature()
  {
    final RowSignature.Builder builder =
        RowSignature.builder().addAll(baseRowSignature);

    final RowSignature baseSignature = builder.build();

    for (Map.Entry<String, ExpressionAndTypeHint> virtualColumn : virtualColumnsByName.entrySet()) {
      final String columnName = virtualColumn.getKey();

      // this is expensive, maybe someday it could use the typeHint, or the inferred type, but for now use native
      // expression type inference
      builder.add(
          columnName,
          virtualColumn.getValue().getExpression().toVirtualColumn(
              columnName,
              virtualColumn.getValue().getTypeHint(),
              macroTable
          ).capabilities(baseSignature, columnName).toColumnType()
      );
    }

    return builder.build();
  }

  /**
   * Given a list of column names, find any corresponding {@link VirtualColumn} with the same name
   */
  public List<DruidExpression> findVirtualColumnExpressions(List<String> allColumns)
  {
    return allColumns.stream()
                     .filter(this::isVirtualColumnDefined)
                     .map(name -> virtualColumnsByName.get(name).getExpression())
                     .collect(Collectors.toList());
  }

  public void visitAllSubExpressions(DruidExpression.DruidExpressionShuttle shuttle)
  {
    for (Map.Entry<String, ExpressionAndTypeHint> entry : virtualColumnsByName.entrySet()) {
      final String key = entry.getKey();
      final ExpressionAndTypeHint wrapped = entry.getValue();
      virtualColumnsByExpression.remove(wrapped);
      final List<DruidExpression> newArgs = shuttle.visitAll(wrapped.getExpression().getArguments());
      final ExpressionAndTypeHint newWrapped = wrap(wrapped.getExpression().withArguments(newArgs), wrapped.getTypeHint());
      virtualColumnsByName.put(key, newWrapped);
      virtualColumnsByExpression.put(newWrapped, key);
    }
  }

  public Collection<? extends VirtualColumn> getAllVirtualColumns(List<String> requiredColumns)
  {
    return requiredColumns.stream()
                          .filter(this::isVirtualColumnDefined)
                          .map(this::getVirtualColumn)
                          .collect(Collectors.toList());
  }

  /**
   * @deprecated use {@link #findVirtualColumnExpressions(List)} instead
   */
  @Deprecated
  public List<VirtualColumn> findVirtualColumns(List<String> allColumns)
  {
    return allColumns.stream()
                     .filter(this::isVirtualColumnDefined)
                     .map(this::getVirtualColumn)
                     .collect(Collectors.toList());
  }

  /**
   * @deprecated use {@link #getOrCreateVirtualColumnForExpression(DruidExpression, ColumnType)} instead
   */
  @Deprecated
  public VirtualColumn getOrCreateVirtualColumnForExpression(
      PlannerContext plannerContext,
      DruidExpression expression,
      ColumnType valueType
  )
  {
    final String name = getOrCreateVirtualColumnForExpression(expression, valueType);
    return virtualColumnsByName.get(name).expression.toVirtualColumn(name, valueType, macroTable);
  }

  /**
   * @deprecated use {@link #getOrCreateVirtualColumnForExpression(DruidExpression, RelDataType)} instead
   */
  @Deprecated
  public VirtualColumn getOrCreateVirtualColumnForExpression(
      PlannerContext plannerContext,
      DruidExpression expression,
      RelDataType dataType
  )
  {
    return getOrCreateVirtualColumnForExpression(
        plannerContext,
        expression,
        Calcites.getColumnTypeForRelDataType(dataType)
    );
  }

  /**
   * @deprecated use {@link #getVirtualColumnByExpression(DruidExpression, RelDataType)} instead
   */
  @Deprecated
  @Nullable
  public VirtualColumn getVirtualColumnByExpression(String expression, RelDataType type)
  {
    final ColumnType columnType = Calcites.getColumnTypeForRelDataType(type);
    ExpressionAndTypeHint wrapped = wrap(DruidExpression.fromExpression(expression), columnType);
    return Optional.ofNullable(virtualColumnsByExpression.get(wrapped))
                   .map(this::getVirtualColumn)
                   .orElse(null);
  }

  private static ExpressionAndTypeHint wrap(DruidExpression expression, ColumnType typeHint)
  {
    return new ExpressionAndTypeHint(expression, typeHint);
  }

  private static class ExpressionAndTypeHint
  {
    private final DruidExpression expression;
    private final ColumnType typeHint;

    public ExpressionAndTypeHint(DruidExpression expression, ColumnType valueType)
    {
      this.expression = expression;
      this.typeHint = valueType;
    }

    public DruidExpression getExpression()
    {
      return expression;
    }

    public ColumnType getTypeHint()
    {
      return typeHint;
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
      ExpressionAndTypeHint expressionAndTypeHint = (ExpressionAndTypeHint) o;
      return Objects.equals(typeHint, expressionAndTypeHint.typeHint) &&
             Objects.equals(expression, expressionAndTypeHint.expression);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(expression, typeHint);
    }
  }
}
