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
import com.google.common.base.Suppliers;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class ExpressionTransform implements Transform
{
  private final String name;
  private final String expression;
  private final ExprMacroTable macroTable;
  private final Supplier<Expr> parsedExpression;

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
    this.parsedExpression = Suppliers.memoize(
        () -> Parser.parse(expression, Preconditions.checkNotNull(this.macroTable, "macroTable"))
    )::get;
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
    return new ExpressionRowFunction(name, parsedExpression.get());
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return parsedExpression.get().analyzeInputs().getRequiredBindings();
  }

  static class ExpressionRowFunction implements RowFunction
  {
    private final String name;
    private final Expr expr;

    ExpressionRowFunction(final String name, final Expr expr)
    {
      this.name = name;
      this.expr = expr;
    }

    @Override
    public Object eval(final Row row)
    {
      try {
        return expr.eval(InputBindings.forRow(row)).valueOrDefault();
      }
      catch (Throwable t) {
        throw new ISE(t, "Could not transform value for %s reason: %s", name, t.getMessage());
      }
    }

    @Override
    public List<String> evalDimension(Row row)
    {
      try {
        return Rows.objectToStrings(expr.eval(InputBindings.forRow(row)).valueOrDefault());
      }
      catch (Throwable t) {
        throw new ISE(t, "Could not transform dimension value for %s reason: %s", name, t.getMessage());
      }
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
