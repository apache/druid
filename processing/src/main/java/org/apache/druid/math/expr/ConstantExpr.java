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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.VectorProcessors;
import org.apache.druid.segment.column.TypeStrategy;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Base type for all constant expressions. {@link ConstantExpr} allow for direct value extraction without evaluating
 * {@link Expr.ObjectBinding}. {@link ConstantExpr} are terminal nodes of an expression tree, and have no children
 * {@link Expr}.
 */
abstract class ConstantExpr<T> implements Expr
{
  final ExpressionType outputType;
  @Nullable
  final T value;

  protected ConstantExpr(ExpressionType outputType, @Nullable T value)
  {
    this.outputType = outputType;
    this.value = value;
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
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
    return BindingAnalysis.EMTPY;
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

/**
 * Even though expressions don't generally support BigInteger, we need this object so we can represent
 * {@link Long#MIN_VALUE} as a {@link UnaryMinusExpr} applied to {@link ConstantExpr}. Antlr cannot parse negative
 * longs directly, due to ambiguity between negative numbers and unary minus.
 */
class BigIntegerExpr extends ConstantExpr<BigInteger>
{
  public BigIntegerExpr(BigInteger value)
  {
    super(ExpressionType.LONG, Preconditions.checkNotNull(value, "value"));
  }

  @Override
  public String toString()
  {
    return value.toString();
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    // Eval succeeds if the BigInteger is in long range.
    // Callers that need to process out-of-long-range values, like UnaryMinusExpr, must use getLiteralValue().
    return ExprEval.ofLong(value.longValueExact());
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    // No vectorization needed: Parser.flatten converts BigIntegerExpr to LongExpr at parse time.
    return false;
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
    BigIntegerExpr otherExpr = (BigIntegerExpr) o;
    return value.equals(otherExpr.value);
  }

  @Override
  public int hashCode()
  {
    return value.hashCode();
  }
}

class LongExpr extends ConstantExpr<Long>
{
  private final ExprEval expr;

  LongExpr(Long value)
  {
    super(ExpressionType.LONG, Preconditions.checkNotNull(value, "value"));
    expr = ExprEval.ofLong(value);
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return expr;
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constant(value, inspector.getMaxVectorSize());
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
    super(ExpressionType.LONG, null);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLong(null);
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constant((Long) null, inspector.getMaxVectorSize());
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

class DoubleExpr extends ConstantExpr<Double>
{
  private final ExprEval expr;

  DoubleExpr(Double value)
  {
    super(ExpressionType.DOUBLE, Preconditions.checkNotNull(value, "value"));
    expr = ExprEval.ofDouble(value);
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return expr;
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constant(value, inspector.getMaxVectorSize());
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
    super(ExpressionType.DOUBLE, null);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDouble(null);
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constant((Double) null, inspector.getMaxVectorSize());
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

class StringExpr extends ConstantExpr<String>
{
  private final ExprEval expr;

  StringExpr(@Nullable String value)
  {
    super(ExpressionType.STRING, NullHandling.emptyToNullIfNeeded(value));
    expr = ExprEval.of(value);
  }

  @Override
  public String toString()
  {
    return value;
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return expr;
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.constant(value, inspector.getMaxVectorSize());
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

class ArrayExpr extends ConstantExpr<Object[]>
{
  public ArrayExpr(ExpressionType outputType, @Nullable Object[] value)
  {
    super(outputType, value);
    Preconditions.checkArgument(outputType.isArray(), "Output type %s is not an array", outputType);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofArray(outputType, value);
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return false;
  }

  @Override
  public String stringify()
  {
    if (value == null) {
      return NULL_LITERAL;
    }
    if (value.length == 0) {
      return outputType.asTypeString() + "[]";
    }
    if (outputType.getElementType().is(ExprType.STRING)) {
      return StringUtils.format(
          "%s[%s]",
          outputType.asTypeString(),
          ARG_JOINER.join(
              Arrays.stream(value)
                    .map(s -> s == null
                              ? NULL_LITERAL
                              // escape as javascript string since string literals are wrapped in single quotes
                              : StringUtils.format("'%s'", StringEscapeUtils.escapeJavaScript((String) s))
                    )
                    .iterator()
          )
      );
    } else if (outputType.getElementType().isNumeric()) {
      return outputType.asTypeString() + Arrays.toString(value);
    } else if (outputType.getElementType().is(ExprType.COMPLEX)) {
      Object[] stringified = new Object[value.length];
      for (int i = 0; i < value.length; i++) {
        stringified[i] = new ComplexExpr((ExpressionType) outputType.getElementType(), value[i]).stringify();
      }
      // use array function to rebuild since we can't stringify complex types directly
      return StringUtils.format("array(%s)", Arrays.toString(stringified));
    } else if (outputType.getElementType().isArray()) {
      // use array function to rebuild since the parser can't yet recognize nested arrays e.g. [['foo', 'bar'],['baz']]
      Object[] stringified = new Object[value.length];
      for (int i = 0; i < value.length; i++) {
        stringified[i] = new ArrayExpr((ExpressionType) outputType.getElementType(), (Object[]) value[i]).stringify();
      }
      return StringUtils.format("array(%s)", Arrays.toString(stringified));
    }
    throw new IAE("cannot stringify array type %s", outputType);
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
    ArrayExpr that = (ArrayExpr) o;
    return outputType.equals(that.outputType) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(outputType, Arrays.hashCode(value));
  }

  @Override
  public String toString()
  {
    return Arrays.toString(value);
  }
}

class ComplexExpr extends ConstantExpr<Object>
{
  private final ExprEval expr;

  protected ComplexExpr(ExpressionType outputType, @Nullable Object value)
  {
    super(outputType, value);
    expr = ExprEval.ofComplex(outputType, value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return expr;
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return false;
  }

  @Override
  public String stringify()
  {
    if (value == null) {
      return StringUtils.format("complex_decode_base64('%s', %s)", outputType.getComplexTypeName(), NULL_LITERAL);
    }
    TypeStrategy strategy = outputType.getStrategy();
    byte[] bytes = new byte[strategy.estimateSizeBytes(value)];
    ByteBuffer wrappedBytes = ByteBuffer.wrap(bytes);
    int remaining = strategy.write(wrappedBytes, 0, value, bytes.length);
    if (remaining < 0) {
      bytes = new byte[bytes.length - remaining];
      wrappedBytes = ByteBuffer.wrap(bytes);
      strategy.write(wrappedBytes, 0, value, bytes.length);
    }
    return StringUtils.format(
        "complex_decode_base64('%s', '%s')",
        outputType.getComplexTypeName(),
        StringUtils.encodeBase64String(bytes)
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
    ComplexExpr that = (ComplexExpr) o;
    return outputType.equals(that.outputType) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(outputType, value);
  }
}
