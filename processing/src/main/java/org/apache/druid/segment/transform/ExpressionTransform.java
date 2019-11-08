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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Row;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.virtual.ExpressionSelectors;

import java.util.List;
import java.util.Objects;

public class ExpressionTransform implements Transform
{
  private final String name;
  private final String expression;
  private final ExprMacroTable macroTable;

  @JsonCreator
  public ExpressionTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("expression") final String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.macroTable = macroTable;
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public RowFunction getRowFunction()
  {
    final Expr expr = Parser.parse(expression, Preconditions.checkNotNull(this.macroTable, "macroTable"));
    return new ExpressionRowFunction(expr);
  }

  static class ExpressionRowFunction implements RowFunction
  {
    private final Expr expr;

    ExpressionRowFunction(final Expr expr)
    {
      this.expr = expr;
    }

    @Override
    public Object eval(final Row row)
    {
      return ExpressionSelectors.coerceEvalToSelectorObject(expr.eval(name -> getValueFromRow(row, name)));
    }
  }

  private static Object getValueFromRow(final Row row, final String column)
  {
    if (column.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return row.getTimestampFromEpoch();
    } else {
      Object raw = row.getRaw(column);
      if (raw instanceof List) {
        return ExpressionSelectors.coerceListToArray((List) raw);
      }
      return raw;
    }
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExpressionTransform that = (ExpressionTransform) o;
    return Objects.equals(name, that.name) &&
           Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, expression);
  }

  @Override
  public String toString()
  {
    return "ExpressionTransform{" +
           "name='" + name + '\'' +
           ", expression='" + expression + '\'' +
           '}';
  }
}
