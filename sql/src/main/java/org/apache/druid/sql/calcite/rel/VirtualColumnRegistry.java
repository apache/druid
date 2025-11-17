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
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.ExpressionParser;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides facilities to create and re-use {@link VirtualColumn} definitions for dimensions, filters, and filtered
 * aggregators while constructing a {@link DruidQuery}.
 */
public class VirtualColumnRegistry
{
  private final ExpressionParser expressionParser;
  private final RowSignature baseRowSignature;
  private final Map<ExpressionAndTypeHint, String> virtualColumnsByExpression;
  private final Map<String, ExpressionAndTypeHint> virtualColumnsByName;
  private final String virtualColumnPrefix;
  private final boolean forceExpressionVirtualColumns;
  private int virtualColumnCounter;

  private VirtualColumnRegistry(
      RowSignature baseRowSignature,
      ExpressionParser expressionParser,
      String virtualColumnPrefix,
      boolean forceExpressionVirtualColumns,
      Map<ExpressionAndTypeHint, String> virtualColumnsByExpression,
      Map<String, ExpressionAndTypeHint> virtualColumnsByName
  )
  {
    this.expressionParser = expressionParser;
    this.baseRowSignature = baseRowSignature;
    this.virtualColumnPrefix = virtualColumnPrefix;
    this.virtualColumnsByExpression = virtualColumnsByExpression;
    this.virtualColumnsByName = virtualColumnsByName;
    this.forceExpressionVirtualColumns = forceExpressionVirtualColumns;
  }

  public static VirtualColumnRegistry create(
      final RowSignature rowSignature,
      final ExpressionParser expressionParser,
      final boolean forceExpressionVirtualColumns
  )
  {
    return new VirtualColumnRegistry(
        rowSignature,
        expressionParser,
        Calcites.findUnusedPrefixForDigits("v", rowSignature.getColumnNames()),
        forceExpressionVirtualColumns,
        new HashMap<>(),
        new HashMap<>()
    );
  }

  public boolean isEmpty()
  {
    return virtualColumnsByExpression.isEmpty();
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
      virtualColumnsByExpression.put(candidate, virtualColumnName);
      virtualColumnsByName.put(virtualColumnName, candidate);
      specialize(virtualColumnName, candidate);
      return virtualColumnName;
    } else {
      return virtualColumnsByExpression.get(candidate);
    }
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
           ? expression.toExpressionVirtualColumn(virtualColumnName, columnType, expressionParser)
           : expression.toVirtualColumn(virtualColumnName, columnType, expressionParser);
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

      final ColumnType typeHint = virtualColumn.getValue().getTypeHint();
      // this is expensive, maybe someday it could use the typeHint, or the inferred type, but for now use native
      // expression type inference
      final ColumnCapabilities virtualCapabilities = virtualColumn.getValue().getExpression().toVirtualColumn(
          columnName,
          typeHint,
          expressionParser
      ).capabilities(baseSignature, columnName);

      // fall back to type hint
      builder.add(
          columnName,
          virtualCapabilities != null ? virtualCapabilities.toColumnType() : typeHint
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

  public Collection<? extends VirtualColumn> getAllVirtualColumns(List<String> requiredColumns)
  {
    return requiredColumns.stream()
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

  private static ExpressionAndTypeHint wrap(DruidExpression expression, ColumnType typeHint)
  {
    return new ExpressionAndTypeHint(expression, typeHint);
  }

  /**
   * Wrapper class for a {@link DruidExpression} and the output {@link ColumnType} "hint" that callers can specify when
   * adding a virtual column with {@link #getOrCreateVirtualColumnForExpression(DruidExpression, RelDataType)} or
   * {@link #getOrCreateVirtualColumnForExpression(DruidExpression, ColumnType)}. This "hint"  will be passed into
   * {@link DruidExpression#toVirtualColumn(String, ColumnType, ExpressionParser)}.
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

  public VirtualColumns build(Set<String> exclude)
  {
    List<VirtualColumn> columns = new ArrayList<>();
    if (virtualColumnsByName == null) {
      return VirtualColumns.EMPTY;
    }

    for (Entry<String, ExpressionAndTypeHint> entry : virtualColumnsByName.entrySet()) {
      if (exclude.contains(entry.getKey())) {
        continue;
      }
      columns.add(getVirtualColumn(entry.getKey()));
    }
    columns.sort(Comparator.comparing(VirtualColumn::getOutputName));
    return VirtualColumns.create(columns);
  }

  /**
   * Called to specialize subexpressions of a new virtual column immediately after adding it. Specialization is
   * recursive: this function may create chains of virtual columns that call into each other.
   */
  private void specialize(final String name, final ExpressionAndTypeHint expressionAndTypeHint)
  {
    if (forceExpressionVirtualColumns) {
      return;
    }

    final Queue<NonnullPair<String, ExpressionAndTypeHint>> toVisit =
        new ArrayDeque<>(Collections.singletonList(new NonnullPair<>(name, expressionAndTypeHint)));
    final SpecializationShuttle shuttle = new SpecializationShuttle();

    while (!toVisit.isEmpty()) {
      final NonnullPair<String, ExpressionAndTypeHint> entry = toVisit.poll();
      final String virtualColumnName = entry.lhs;
      final ExpressionAndTypeHint expression = entry.rhs;
      final List<DruidExpression> newArgs = shuttle.visitAll(expression.getExpression().getArguments());
      ExpressionAndTypeHint newExpression = wrap(
          shuttle.visit(expression.getExpression().withArguments(newArgs)),
          expression.getTypeHint()
      );

      // If the expression becomes a direct access of another virtual column after rewriting, then map this
      // virtual column name to the expression for the referenced virtual column.
      if (newExpression.getExpression().isDirectColumnAccess()
          && virtualColumnsByName.containsKey(newExpression.getExpression().getDirectColumn())) {
        newExpression = virtualColumnsByName.get(newExpression.getExpression().getDirectColumn());
      }

      // Map both the old and new expression to the same virtual column name.
      virtualColumnsByName.put(virtualColumnName, newExpression);
      virtualColumnsByExpression.put(expression, virtualColumnName);
      virtualColumnsByExpression.putIfAbsent(newExpression, virtualColumnName);
    }
  }

  /**
   * Shuttle used by {@link #specialize(String, ExpressionAndTypeHint)}.
   */
  private class SpecializationShuttle implements DruidExpression.DruidExpressionShuttle
  {
    @Override
    public DruidExpression visit(DruidExpression expression)
    {
      if (expression.getType() == DruidExpression.NodeType.SPECIALIZED) {
        // add the expression to the top level of the registry as a standalone virtual column
        final String name = getOrCreateVirtualColumnForExpression(expression, expression.getDruidType());
        // replace with an identifier expression of the new virtual column name
        return DruidExpression.ofColumn(expression.getDruidType(), name);
      } else {
        // do nothing
        return expression;
      }
    }
  }
}
