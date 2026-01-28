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

package org.apache.druid.math.expr;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class LambdaExpr implements Expr
{
  private final ImmutableList<IdentifierExpr> args;
  private final Expr expr;

  LambdaExpr(List<IdentifierExpr> args, Expr expr)
  {
    this.args = ImmutableList.copyOf(args);
    this.expr = expr;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s -> %s)", args, expr);
  }

  int identifierCount()
  {
    return args.size();
  }

  @Nullable
  public String getIdentifier()
  {
    Preconditions.checkState(args.size() < 2, "LambdaExpr has multiple arguments, use getIdentifiers");
    if (args.size() == 1) {
      return args.get(0).toString();
    }
    return null;
  }

  public List<String> getIdentifiers()
  {
    return args.stream().map(IdentifierExpr::toString).collect(Collectors.toList());
  }

  public List<String> stringifyIdentifiers()
  {
    return args.stream().map(IdentifierExpr::stringify).collect(Collectors.toList());
  }

  ImmutableList<IdentifierExpr> getIdentifierExprs()
  {
    return args;
  }

  public Expr getExpr()
  {
    return expr;
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return expr.canVectorize(inspector);
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return expr.asVectorProcessor(inspector);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return expr.eval(bindings);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("(%s) -> %s", ARG_JOINER.join(stringifyIdentifiers()), expr.stringify());
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    List<IdentifierExpr> newArgs =
        args.stream().map(arg -> (IdentifierExpr) shuttle.visit(arg)).collect(Collectors.toList());
    Expr newBody = expr.visit(shuttle);
    return shuttle.visit(new LambdaExpr(newArgs, newBody));
  }

  @Override
  public BindingAnalysis analyzeInputs()
  {
    final Set<String> lambdaArgs = args.stream().map(IdentifierExpr::toString).collect(Collectors.toSet());
    BindingAnalysis bodyDetails = expr.analyzeInputs();
    return bodyDetails.removeLambdaArguments(lambdaArgs);
  }

  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return expr.getOutputType(inspector);
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
    LambdaExpr that = (LambdaExpr) o;
    return Objects.equals(args, that.args) &&
           Objects.equals(expr, that.expr);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(args, expr);
  }
}
