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

package org.apache.druid.java.util.common.parsers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

public class JSONPathFieldSpec
{
  private final JSONPathFieldType type;
  private final String name;
  private final String expr;

  @JsonCreator
  public JSONPathFieldSpec(
      @JsonProperty("type") JSONPathFieldType type,
      @JsonProperty("name") String name,
      @JsonProperty("expr") String expr
  )
  {
    this.type = type;
    this.name = Preconditions.checkNotNull(name, "Missing 'name' in field spec");

    // If expr is null and type is root, use the name as the expr too.
    if (expr == null && type == JSONPathFieldType.ROOT) {
      this.expr = name;
    } else {
      this.expr = Preconditions.checkNotNull(expr, "Missing 'expr' for field[%s]", name);
    }
  }

  @JsonProperty
  public JSONPathFieldType getType()
  {
    return type;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getExpr()
  {
    return expr;
  }

  @JsonCreator
  public static JSONPathFieldSpec fromString(String name)
  {
    return JSONPathFieldSpec.createRootField(name);
  }

  public static JSONPathFieldSpec createNestedField(String name, String expr)
  {
    return new JSONPathFieldSpec(JSONPathFieldType.PATH, name, expr);
  }

  public static JSONPathFieldSpec createJqField(String name, String expr)
  {
    return new JSONPathFieldSpec(JSONPathFieldType.JQ, name, expr);
  }

  public static JSONPathFieldSpec createRootField(String name)
  {
    return new JSONPathFieldSpec(JSONPathFieldType.ROOT, name, null);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JSONPathFieldSpec that = (JSONPathFieldSpec) o;
    return type == that.type &&
           Objects.equals(name, that.name) &&
           Objects.equals(expr, that.expr);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, name, expr);
  }

  @Override
  public String toString()
  {
    return "JSONPathFieldSpec{" +
           "type=" + type +
           ", name='" + name + '\'' +
           ", expr='" + expr + '\'' +
           '}';
  }
}
