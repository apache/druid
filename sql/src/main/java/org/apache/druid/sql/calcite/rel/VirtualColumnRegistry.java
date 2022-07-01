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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
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
  private boolean forceExpressionVirtualColumns;

  private VirtualColumnRegistry(
      RowSignature baseRowSignature,
      ExprMacroTable macroTable,
      String virtualColumnPrefix,
      boolean forceExpressionVirtualColumns,
      Map<ExpressionAndTypeHint, String> virtualColumnsByExpression,
      Map<String, ExpressionAndTypeHint> virtualColumnsByName
  )
  {
    this.macroTable = macroTable;
    this.baseRowSignature = baseRowSignature;
    this.virtualColumnPrefix = virtualColumnPrefix;
    this.virtualColumnsByExpression = virtualColumnsByExpression;
    this.virtualColumnsByName = virtualColumnsByName;
    this.forceExpressionVirtualColumns = forceExpressionVirtualColumns;
  }

  public static VirtualColumnRegistry create(
      final RowSignature rowSignature,
      final ExprMacroTable macroTable,
      final boolean forceExpressionVirtualColumns
  )
  {
    return new VirtualColumnRegistry(
        rowSignature,
        macroTable,
        Calcites.findUnusedPrefixForDigits("v", rowSignature.getColumnNames()),
        forceExpressionVirtualColumns,
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
    if (typeHint.getSqlTypeName() == SqlTypeName.OTHER && expression.getDruidType() != null) {
      // fall back to druid type if sql type isn't very helpful
      return getOrCreateVirtualColumnForExpression(
          expression,
          expression.getDruidType()
      );
    }
    return getOrCreateVirtualColumnForExpression(
        expression,
        Calcites.getColumnTypeForRelDataType(typeHint)
    );
  }

  /**
   * Get existing virtual column by column name.
   *
   * @return null if a virtual column for the given name does not exist.
   */
  @Nullable
  public VirtualColumn getVirtualColumn(String virtualColumnName)
  {
    ExpressionAndTypeHint registeredColumn = virtualColumnsByName.get(virtualColumnName);
    if (registeredColumn == null) {
      return null;
    }

    DruidExpression expression = registeredColumn.getExpression();
    ColumnType columnType = registeredColumn.getTypeHint();
    return forceExpressionVirtualColumns
           ? expression.toExpressionVirtualColumn(virtualColumnName, columnType, macroTable)
           : expression.toVirtualColumn(virtualColumnName, columnType, macroTable);
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
    final Queue<Map.Entry<String, ExpressionAndTypeHint>> toVisit = new ArrayDeque<>(virtualColumnsByName.entrySet());
    while (!toVisit.isEmpty()) {
      final Map.Entry<String, ExpressionAndTypeHint> entry = toVisit.poll();
      final String key = entry.getKey();
      final ExpressionAndTypeHint wrapped = entry.getValue();
      final List<DruidExpression> newArgs = shuttle.visitAll(wrapped.getExpression().getArguments());
      final ExpressionAndTypeHint newWrapped = wrap(wrapped.getExpression().withArguments(newArgs), wrapped.getTypeHint());
      virtualColumnsByName.put(key, newWrapped);
      virtualColumnsByExpression.remove(wrapped);
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
    return getVirtualColumn(name);
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

  /**
   * Wrapper class for a {@link DruidExpression} and the output {@link ColumnType} "hint" that callers can specify when
   * adding a virtual column with {@link #getOrCreateVirtualColumnForExpression(DruidExpression, RelDataType)} or
   * {@link #getOrCreateVirtualColumnForExpression(DruidExpression, ColumnType)}. This "hint"  will be passed into
   * {@link DruidExpression#toVirtualColumn(String, ColumnType, ExprMacroTable)}.
   *
   * The type hint might be different than {@link DruidExpression#getDruidType()} since that value is the captured value
   * of {@link org.apache.calcite.rex.RexNode#getType()} converted to the Druid type system, while callers might still
   * explicitly specify a different type to use for the hint. Additionally, the method used to convert Calcite types to
   * Druid types does not completely map the former to the latter, and the method typically used to do the conversion,
   * {@link Calcites#getColumnTypeForRelDataType(RelDataType)}, might return null, where the caller might know what
   * the type should be.
   */
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
