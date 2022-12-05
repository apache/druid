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

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.vector.ExprEvalDoubleVector;
import org.apache.druid.math.expr.vector.ExprEvalLongVector;
import org.apache.druid.math.expr.vector.ExprEvalObjectVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This {@link Expr} node is used to represent a variable in the expression language. At evaluation time, the string
 * identifier will be used to retrieve the runtime value for the variable from {@link Expr.ObjectBinding}.
 * {@link IdentifierExpr} are terminal nodes of an expression tree, and have no children {@link Expr}.
 */
class IdentifierExpr implements Expr
{
  final String identifier;
  final String binding;

  /**
   * Construct a identifier expression for a {@link LambdaExpr}, where the {@link #identifier} is equal to
   * {@link #binding}
   */
  IdentifierExpr(String value)
  {
    this.identifier = value;
    this.binding = value;
  }

  /**
   * Construct a normal identifier expression, where {@link #binding} is the key to fetch the backing value from
   * {@link Expr.ObjectBinding} and the {@link #identifier} is a unique string that identifies this usage of the
   * binding.
   */
  IdentifierExpr(String identifier, String binding)
  {
    this.identifier = identifier;
    this.binding = binding;
  }

  @Override
  public String toString()
  {
    return binding;
  }

  /**
   * Unique identifier for the binding
   */
  @Nullable
  public String getIdentifier()
  {
    return identifier;
  }

  /**
   * Value binding, key to retrieve value from {@link Expr.ObjectBinding#get(String)}
   */
  @Nullable
  public String getBinding()
  {
    return binding;
  }

  @Override
  public boolean isIdentifier()
  {
    return true;
  }

  @Nullable
  @Override
  public String getIdentifierIfIdentifier()
  {
    return identifier;
  }

  @Nullable
  @Override
  public String getBindingIfIdentifier()
  {
    return binding;
  }

  @Nullable
  @Override
  public IdentifierExpr getIdentifierExprIfIdentifierExpr()
  {
    return this;
  }

  @Override
  public BindingAnalysis analyzeInputs()
  {
    return new BindingAnalysis(this);
  }

  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return inspector.getType(binding);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofType(bindings.getType(binding), bindings.get(binding));
  }

  @Override
  public String stringify()
  {
    // escape as java strings since identifiers are wrapped in double quotes
    return StringUtils.format("\"%s\"", StringEscapeUtils.escapeJava(binding));
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    return shuttle.visit(this);
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return true;
  }

  @Override
  public ExprVectorProcessor<?> buildVectorized(VectorInputBindingInspector inspector)
  {
    ExpressionType inputType = inspector.getType(binding);

    if (inputType == null) {
      // nil column, we can be anything, so be a string because it's the most flexible
      // (numbers will be populated with default values in default mode and non-null)
      return new IdentifierVectorProcessor<Object[]>(ExpressionType.STRING)
      {
        @Override
        public ExprEvalVector<Object[]> evalVector(VectorInputBinding bindings)
        {
          return new ExprEvalObjectVector(bindings.getObjectVector(binding));
        }
      };
    }
    switch (inputType.getType()) {
      case LONG:
        return new IdentifierVectorProcessor<long[]>(inputType)
        {
          @Override
          public ExprEvalVector<long[]> evalVector(VectorInputBinding bindings)
          {
            return new ExprEvalLongVector(bindings.getLongVector(binding), bindings.getNullVector(binding));
          }
        };
      case DOUBLE:
        return new IdentifierVectorProcessor<double[]>(inputType)
        {
          @Override
          public ExprEvalVector<double[]> evalVector(VectorInputBinding bindings)
          {
            return new ExprEvalDoubleVector(bindings.getDoubleVector(binding), bindings.getNullVector(binding));
          }
        };
      case STRING:
        return new IdentifierVectorProcessor<Object[]>(inputType)
        {
          @Override
          public ExprEvalVector<Object[]> evalVector(VectorInputBinding bindings)
          {
            return new ExprEvalObjectVector(bindings.getObjectVector(binding));
          }
        };
      default:
        throw Exprs.cannotVectorize(this);
    }
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
    IdentifierExpr that = (IdentifierExpr) o;
    return Objects.equals(identifier, that.identifier);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(identifier);
  }
}

abstract class IdentifierVectorProcessor<T> implements ExprVectorProcessor<T>
{
  private final ExpressionType outputType;

  public IdentifierVectorProcessor(ExpressionType outputType)
  {
    this.outputType = outputType;
  }

  @Override
  public ExpressionType getOutputType()
  {
    return outputType;
  }
}

