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
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

  /**
   * Empty {@link Expr.ObjectBinding} that doesn't complain about attempts to access type or value for any input
   * identifiers, both of which will be nulls. Typically used for evaluating known constant expressions, or finding
   * a default or initial value of some expression if all inputs are null.
   */
  public static Expr.ObjectBinding nilBindings()
  {
    return NIL_BINDINGS;
  }

  /**
   * Empty binding that throw a {@link UOE} if anything attempts to lookup an identifier type or value
   */
  public static Expr.ObjectBinding validateConstant(Expr expr)
  {
    return new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        // Sanity check. Bindings should not be used for a constant expression so explode if something tried
        throw new UOE("Expression " + expr.stringify() + " has non-constant inputs.");
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        // Sanity check. Bindings should not be used for a constant expression so explode if something tried
        throw new UOE("Expression " + expr.stringify() + " has non-constant inputs.");
      }
    };
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

  public static Expr.InputBindingInspector inspectorForColumn(String column, ExpressionType type)
  {
    return new Expr.InputBindingInspector()
    {
      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        if (column.equals(name)) {
          return type;
        }
        return null;
      }
    };
  }

  /**
   * Creates a {@link Expr.ObjectBinding} backed by some {@link Row}. {@link ColumnHolder#TIME_COLUMN_NAME} is special
   * handled to be backed by {@link Row#getTimestampFromEpoch()}, all other values are ethically sourced from
   * {@link Row#getRaw(String)}.
   *
   * Types are detected and values are coereced via {@link ExprEval#bestEffortOf(Object)} because input types are
   * currently unknown.
   */
  public static Expr.ObjectBinding forRow(Row row)
  {
    return new BestEffortInputBindings()
    {
      @Override
      ExprEval compute(String name)
      {
        if (ColumnHolder.TIME_COLUMN_NAME.equals(name)) {
          return ExprEval.ofLong(row.getTimestampFromEpoch());
        }
        return ExprEval.bestEffortOf(row.getRaw(name));
      }
    };
  }

  /**
   * Create {@link Expr.ObjectBinding} backed by {@link Map} to provide values for identifiers to evaluate {@link Expr}
   *
   * Types are detected and values are coereced via {@link ExprEval#bestEffortOf(Object)} because input types are
   * currently unknown.
   *
   * This method is only used for testing and mimics the behavior of {@link #forRow(Row)} except lacks special handling
   * for columns named {@link ColumnHolder#TIME_COLUMN_NAME}.
   */
  @Deprecated
  public static Expr.ObjectBinding forMap(final Map<String, ?> bindings)
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
   * Create {@link Expr.ObjectBinding} backed by {@link Map} to provide values for identifiers to evaluate {@link Expr}
   *
   * Types are detected and values are coereced via {@link ExprEval#bestEffortOf(Object)} because input types are
   * currently unknown.
   */
  public static Expr.ObjectBinding forMap(final Map<String, ?> bindings, Expr.InputBindingInspector inspector)
  {
    final Expr.InputBindingInspector inputBindingInspector = inspector;
    return new BestEffortInputBindings()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        if (inputBindingInspector.getType(name) != null) {
          return bindings.get(name);
        }
        // we didn't have complete type information on this one, fall through to bestEffortOf
        return super.get(name);
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        final ExpressionType type = inputBindingInspector.getType(name);
        if (type != null) {
          return type;
        }
        // we didn't have complete type information on this one, fall through to bestEffortOf
        return super.getType(name);
      }

      @Override
      ExprEval compute(String name)
      {
        return ExprEval.bestEffortOf(bindings.get(name));
      }
    };
  }

  /**
   * Create a {@link Expr.ObjectBinding} for a single input value of a known type provided by some {@link Supplier}
   */
  public static Expr.ObjectBinding forInputSupplier(ExpressionType type, Supplier<?> supplier)
  {
    return new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        return supplier.get();
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return type;
      }
    };
  }

  /**
   * Create a {@link Expr.ObjectBinding} for a single input value of a known type provided by some {@link Supplier}
   */
  public static Expr.ObjectBinding forInputSupplier(String supplierName, ExpressionType type, Supplier<?> supplier)
  {
    return new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        if (Objects.equals(name, supplierName)) {
          return supplier.get();
        }
        return null;
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        if (Objects.equals(name, supplierName)) {
          return type;
        }
        return null;
      }
    };
  }

  public static <T> InputSupplier<T> inputSupplier(ExpressionType type, Supplier<T> supplier)
  {
    return new InputSupplier<>(type, supplier);
  }

  /**
   * Create {@link Expr.ObjectBinding} backed by map of {@link Supplier} to provide values for identifiers to evaluate
   * {@link Expr}
   */
  public static Expr.ObjectBinding forInputSuppliers(final Map<String, InputSupplier<?>> bindings)
  {
    return new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        InputSupplier<?> binding = bindings.get(name);
        return binding == null ? null : binding.get();
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        InputSupplier<?> binding = bindings.get(name);
        if (binding == null) {
          return null;
        }
        return binding.getType();
      }
    };
  }

  public static class InputSupplier<T> implements Supplier<T>
  {
    private final ExpressionType type;
    private final Supplier<T> supplier;

    private InputSupplier(ExpressionType type, Supplier<T> supplier)
    {
      this.supplier = supplier;
      this.type = type;
    }

    @Override
    public T get()
    {
      return supplier.get();
    }

    public ExpressionType getType()
    {
      return type;
    }
  }

  /**
   * {@link Expr.ObjectBinding} backed by a cache populated by {@link ExprEval#bestEffortOf(Object)} for when the input
   * type information is totally unknown, for a single row worth of values. The values are cached so that asking for a
   * type and getting the value of some input do not repeat computations.
   *
   * This type is not thread-safe, and not suitable for re-use for processing multiple-rows due to the presence of the
   * result cache.
   */
  public abstract static class BestEffortInputBindings implements Expr.ObjectBinding
  {
    private final Map<String, ExprEval> cachedBindings = new HashMap<>();

    abstract ExprEval compute(String name);

    @Nullable
    @Override
    public Object get(String name)
    {
      cachedBindings.computeIfAbsent(name, this::compute);
      return cachedBindings.get(name).value();
    }

    @Nullable
    @Override
    public ExpressionType getType(String name)
    {
      cachedBindings.computeIfAbsent(name, this::compute);
      return cachedBindings.get(name).type();
    }
  }
}
