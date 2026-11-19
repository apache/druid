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

package org.apache.druid.query.context;

import org.apache.druid.java.util.common.IAE;

import java.util.Objects;

/**
 * The default of a query context parameter: none, a literal value, or the value of a runtime property.
 *
 * @param <T> value type of the parameter
 */
public sealed interface DefaultValue<T>
{
  /**
   * No declared default: the call site supplies the fallback.
   */
  record None<T>() implements DefaultValue<T>
  {
  }

  /**
   * A literal default value.
   */
  record Literal<T>(T value) implements DefaultValue<T>
  {
    public Literal
    {
      Objects.requireNonNull(value, "value");
    }
  }

  /**
   * The default is the value of a runtime property, such as {@code druid.broker.http.maxQueuedBytes}, which the code
   * reading the parameter supplies as its fallback.
   */
  record FromProperty<T>(String property) implements DefaultValue<T>
  {
    public FromProperty
    {
      Objects.requireNonNull(property, "property");
      if (property.isBlank()) {
        throw new IAE("Default property must not be blank");
      }
    }
  }

  static <T> DefaultValue<T> none()
  {
    return new None<>();
  }

  static <T> FromProperty<T> fromProperty(final String property)
  {
    return new FromProperty<>(property);
  }
}
