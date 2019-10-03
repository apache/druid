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

package org.apache.druid.indexer;

import java.util.Collection;
import java.util.Objects;

/**
 * Convenience class for holding a pair of string key and templated value.
 */
public class Property<T>
{
  private final String name;
  private final T value;

  public Property(String name, T value)
  {
    this.name = name;
    this.value = value;
  }

  public String getName()
  {
    return name;
  }

  public T getValue()
  {
    return value;
  }

  public boolean isValueNullOrEmptyCollection()
  {
    if (value == null) {
      return true;
    }
    if (value instanceof Collection) {
      return ((Collection) value).isEmpty();
    }
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
    Property<?> property = (Property<?>) o;
    return Objects.equals(name, property.name) &&
           Objects.equals(value, property.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, value);
  }

  @Override
  public String toString()
  {
    return "Property{" +
           "name='" + name + '\'' +
           ", value=" + value +
           '}';
  }
}
