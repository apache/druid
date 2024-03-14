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

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.TypeStrategy;

import javax.annotation.Nullable;
import java.util.List;

public class BuiltInExprMacros
{
  public static class ComplexDecodeBase64ExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "complex_decode_base64";
    public static final String ALIAS = "decode_base64_complex";

    /**
     * use name() in closure scope to allow Alias macro to override it with alias.
     *
     * @return String
     */
    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      return new ComplexDecodeBase64Expression(args);
    }

    final class ComplexDecodeBase64Expression extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private final ExpressionType complexType;
      private final TypeStrategy<?> typeStrategy;

      public ComplexDecodeBase64Expression(List<Expr> args)
      {
        super(ComplexDecodeBase64ExprMacro.this, args);
        validationHelperCheckArgumentCount(args, 2);
        final Expr arg0 = args.get(0);

        if (!arg0.isLiteral()) {
          throw validationFailed(
              "first argument must be constant STRING expression containing a valid complex type name but got '%s' instead",
              arg0.stringify()
          );
        }
        if (arg0.isNullLiteral()) {
          throw validationFailed("first argument must be constant STRING expression containing a valid complex type name but got NULL instead");
        }
        final Object literal = arg0.getLiteralValue();
        if (!(literal instanceof String)) {
          throw validationFailed(
              "first argument must be constant STRING expression containing a valid complex type name but got '%s' instead",
              arg0.getLiteralValue()
          );
        }

        this.complexType = ExpressionTypeFactory.getInstance().ofComplex((String) literal);
        try {
          this.typeStrategy = complexType.getStrategy();
        }
        catch (IllegalArgumentException illegal) {
          throw validationFailed(
              "first argument must be a valid COMPLEX type name, got unknown COMPLEX type [%s]",
              complexType.asTypeString()
          );
        }
      }

      @Override
      public ExprEval<?> eval(ObjectBinding bindings)
      {
        ExprEval<?> toDecode = args.get(1).eval(bindings);
        if (toDecode.value() == null) {
          return ExprEval.ofComplex(complexType, null);
        }
        final Object serializedValue = toDecode.value();
        final byte[] base64;
        if (serializedValue instanceof String) {
          base64 = StringUtils.decodeBase64String(toDecode.asString());
        } else if (serializedValue instanceof byte[]) {
          base64 = (byte[]) serializedValue;
        } else if (complexType.getComplexTypeName().equals(toDecode.type().getComplexTypeName())) {
          // pass it through, it is already the right thing
          return toDecode;
        } else {
          throw validationFailed(
              "second argument must be a base64 encoded STRING value but got %s instead",
              toDecode.type()
          );
        }

        return ExprEval.ofComplex(complexType, typeStrategy.fromBytes(base64));
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return complexType;
      }

      @Override
      public boolean isLiteral()
      {
        return args.get(1).isLiteral();
      }

      @Override
      public boolean isNullLiteral()
      {
        return args.get(1).isNullLiteral();
      }

      @Nullable
      @Override
      public Object getLiteralValue()
      {
        return eval(InputBindings.nilBindings()).value();
      }
    }
  }

  public static class StringDecodeBase64UTFExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "decode_base64_utf8";

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 1);
      return new StringDecodeBase64UTFExpression(this, args);
    }

    /**
     * use name() in closure scope to allow Alias macro to override it with alias.
     *
     * @return String
     */
    @Override
    public String name()
    {
      return NAME;
    }

    static final class StringDecodeBase64UTFExpression extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private final Expr arg;

      public StringDecodeBase64UTFExpression(StringDecodeBase64UTFExprMacro macro, List<Expr> args)
      {
        super(macro, args);
        this.arg = Iterables.getOnlyElement(args);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        ExprEval<?> toDecode = arg.eval(bindings);
        if (toDecode.value() == null) {
          return ExprEval.of(null);
        }
        return new StringExpr(StringUtils.fromUtf8(StringUtils.decodeBase64String(toDecode.asString()))).eval(bindings);
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.STRING;
      }

      @Override
      public boolean isLiteral()
      {
        return arg.isLiteral();
      }

      @Override
      public boolean isNullLiteral()
      {
        return arg.isNullLiteral();
      }

      @Nullable
      @Override
      public Object getLiteralValue()
      {
        return eval(InputBindings.nilBindings()).value();
      }
    }
  }

}
