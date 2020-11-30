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
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.VectorProcessors;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Base type for all constant expressions. {@link ConstantExpr} allow for direct value extraction without evaluating
 * {@link Expr.ObjectBinding}. {@link ConstantExpr} are terminal nodes of an expression tree, and have no children
 * {@link Expr}.
 */
abstract class ConstantExpr<T> implements Expr
{
  final ExprType outputType;
  @Nullable
  final T value;

  protected ConstantExpr(ExprType outputType, @Nullable T value)
  {
    this.outputType = outputType;
    this.value = value;
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    // null isn't really a type, so don't claim anything
    return value == null ? null : outputType;
  }

  @Override
  public boolean isLiteral()
  {
    return true;
  }

  @Override
  public boolean isNullLiteral()
  {
    return value == null;
  }

  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    return shuttle.visit(this);
  }

  @Override
  public BindingAnalysis analyzeInputs()
  {
    return new BindingAnalysis();
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return true;
  }

  @Override
  public String stringify()
  {
    return toString();
  }
}

class LongExpr extends ConstantExpr<Long>
{
  LongExpr(Long value)
  {
    super(ExprType.LONG, Preconditions.checkNotNull(value, "value"));
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLong(value);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constantLong(value, inspector.getMaxVectorSize());
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
    LongExpr longExpr = (LongExpr) o;
    return Objects.equals(value, longExpr.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }
}

class NullLongExpr extends ConstantExpr<Long>
{
  NullLongExpr()
  {
    super(ExprType.LONG, null);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLong(null);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constantLong(null, inspector.getMaxVectorSize());
  }

  @Override
  public final int hashCode()
  {
    return NullLongExpr.class.hashCode();
  }

  @Override
  public final boolean equals(Object obj)
  {
    return obj instanceof NullLongExpr;
  }

  @Override
  public String toString()
  {
    return NULL_LITERAL;
  }
}

class LongArrayExpr extends ConstantExpr<Long[]>
{
  LongArrayExpr(Long[] value)
  {
    super(ExprType.LONG_ARRAY, Preconditions.checkNotNull(value, "value"));
  }

  @Override
  public String toString()
  {
    return Arrays.toString(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLongArray(value);
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return false;
  }

  @Override
  public String stringify()
  {
    if (value.length == 0) {
      return "<LONG>[]";
    }
    return StringUtils.format("<LONG>%s", toString());
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
    LongArrayExpr that = (LongArrayExpr) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(value);
  }
}

class DoubleExpr extends ConstantExpr<Double>
{
  DoubleExpr(Double value)
  {
    super(ExprType.DOUBLE, Preconditions.checkNotNull(value, "value"));
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDouble(value);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constantDouble(value, inspector.getMaxVectorSize());
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
    DoubleExpr that = (DoubleExpr) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }
}

class NullDoubleExpr extends ConstantExpr<Double>
{
  NullDoubleExpr()
  {
    super(ExprType.DOUBLE, null);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDouble(null);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constantDouble(null, inspector.getMaxVectorSize());
  }

  @Override
  public final int hashCode()
  {
    return NullDoubleExpr.class.hashCode();
  }

  @Override
  public final boolean equals(Object obj)
  {
    return obj instanceof NullDoubleExpr;
  }

  @Override
  public String toString()
  {
    return NULL_LITERAL;
  }
}

class DoubleArrayExpr extends ConstantExpr<Double[]>
{
  DoubleArrayExpr(Double[] value)
  {
    super(ExprType.DOUBLE_ARRAY, Preconditions.checkNotNull(value, "value"));
  }

  @Override
  public String toString()
  {
    return Arrays.toString(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDoubleArray(value);
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return false;
  }

  @Override
  public String stringify()
  {
    if (value.length == 0) {
      return "<DOUBLE>[]";
    }
    return StringUtils.format("<DOUBLE>%s", toString());
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
    DoubleArrayExpr that = (DoubleArrayExpr) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(value);
  }
}

class StringExpr extends ConstantExpr<String>
{
  StringExpr(@Nullable String value)
  {
    super(ExprType.STRING, NullHandling.emptyToNullIfNeeded(value));
  }

  @Override
  public String toString()
  {
    return value;
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.of(value);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constantString(value, inspector.getMaxVectorSize());
  }

  @Override
  public String stringify()
  {
    // escape as javascript string since string literals are wrapped in single quotes
    return value == null ? NULL_LITERAL : StringUtils.format("'%s'", StringEscapeUtils.escapeJavaScript(value));
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
    StringExpr that = (StringExpr) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }
}

class StringArrayExpr extends ConstantExpr<String[]>
{
  StringArrayExpr(String[] value)
  {
    super(ExprType.STRING_ARRAY, Preconditions.checkNotNull(value, "value"));
  }

  @Override
  public String toString()
  {
    return Arrays.toString(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofStringArray(value);
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return false;
  }

  @Override
  public String stringify()
  {
    if (value.length == 0) {
      return "<STRING>[]";
    }

    return StringUtils.format(
        "<STRING>[%s]",
        ARG_JOINER.join(
            Arrays.stream(value)
                  .map(s -> s == null
                            ? NULL_LITERAL
                            // escape as javascript string since string literals are wrapped in single quotes
                            : StringUtils.format("'%s'", StringEscapeUtils.escapeJavaScript(s))
                  )
                  .iterator()
        )
    );
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
    StringArrayExpr that = (StringArrayExpr) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(value);
  }
}
