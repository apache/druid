/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.Parser;

import java.util.Objects;

public class Transform
{
  private final String name;
  private final String expression;
  private final ExprMacroTable macroTable;

  @JsonCreator
  public Transform(
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
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  public Expr toExpr()
  {
    return Parser.parse(expression, Preconditions.checkNotNull(macroTable, "macroTable"));
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
    final Transform transform = (Transform) o;
    return Objects.equals(name, transform.name) &&
           Objects.equals(expression, transform.expression);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, expression);
  }

  @Override
  public String toString()
  {
    return "Transform{" +
           "name='" + name + '\'' +
           ", expression='" + expression + '\'' +
           '}';
  }
}
