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

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.Pair;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Function;

public class InputBindings
{
  private static final Expr.ObjectBinding NIL_BINDINGS = new Expr.ObjectBinding()
  {
    @Nullable
    @Override
    public Object get(String name)
    {
      return null;
    }

    @Nullable
    @Override
    public ExpressionType getType(String name)
    {
      return null;
    }
  };

  public static Expr.ObjectBinding nilBindings()
  {
    return NIL_BINDINGS;
  }

  /**
   * Create an {@link Expr.InputBindingInspector} backed by a map of binding identifiers to their {@link ExprType}
   */
  public static Expr.InputBindingInspector inspectorFromTypeMap(final Map<String, ExpressionType> types)
  {
    return new Expr.InputBindingInspector()
    {
      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return types.get(name);
      }
    };
  }

  public static Expr.ObjectBinding singleProvider(ExpressionType type, final Function<String, ?> valueFn)
  {
    return new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        return valueFn.apply(name);
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return type;
      }
    };
  }

  public static Expr.ObjectBinding forFunction(final Function<String, ?> valueFn)
  {
    return new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        return valueFn.apply(name);
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return ExprEval.bestEffortOf(valueFn.apply(name)).type();
      }
    };
  }

  /**
   * Create {@link Expr.ObjectBinding} backed by {@link Map} to provide values for identifiers to evaluate {@link Expr}
   */
  public static Expr.ObjectBinding withMap(final Map<String, ?> bindings)
  {
    return new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        return bindings.get(name);
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return ExprEval.bestEffortOf(bindings.get(name)).type();
      }
    };
  }

  /**
   * Create {@link Expr.ObjectBinding} backed by map of {@link Supplier} to provide values for identifiers to evaluate
   * {@link Expr}
   */
  public static Expr.ObjectBinding withTypedSuppliers(final Map<String, Pair<ExpressionType, Supplier<Object>>> bindings)
  {
    return new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        Pair<ExpressionType, Supplier<Object>> binding = bindings.get(name);
        return binding == null || binding.rhs == null ? null : binding.rhs.get();
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        Pair<ExpressionType, Supplier<Object>> binding = bindings.get(name);
        if (binding == null) {
          return null;
        }
        return binding.lhs;
      }
    };
  }
}
