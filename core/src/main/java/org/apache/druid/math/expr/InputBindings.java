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

import javax.annotation.Nullable;
import java.util.Map;

public class InputBindings
{
  /**
   * Create an {@link Expr.InputBindingInspector} backed by a map of binding identifiers to their {@link ExprType}
   */
  public static Expr.InputBindingInspector inspectorFromTypeMap(final Map<String, ExprType> types)
  {
    return new Expr.InputBindingInspector()
    {
      @Nullable
      @Override
      public ExprType getType(String name)
      {
        return types.get(name);
      }
    };
  }

  /**
   * Create {@link Expr.ObjectBinding} backed by {@link Map} to provide values for identifiers to evaluate {@link Expr}
   */
  public static Expr.ObjectBinding withMap(final Map<String, ?> bindings)
  {
    return bindings::get;
  }

  /**
   * Create {@link Expr.ObjectBinding} backed by map of {@link Supplier} to provide values for identifiers to evaluate
   * {@link Expr}
   */
  public static Expr.ObjectBinding withSuppliers(final Map<String, Supplier<Object>> bindings)
  {
    return (String name) -> {
      Supplier<Object> supplier = bindings.get(name);
      return supplier == null ? null : supplier.get();
    };
  }
}
