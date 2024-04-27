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
import com.jayway.jsonpath.JsonPath;
import org.apache.druid.utils.CollectionUtils;

import java.util.List;
import java.util.Objects;

public class JSONPathFieldSpec
{
  private final JSONPathFieldType type;
  private final String name;
  private final String expr;
  private final List<String> nodes;

  @JsonCreator
  public JSONPathFieldSpec(
      @JsonProperty("type") JSONPathFieldType type,
      @JsonProperty("name") String name,
      @JsonProperty("expr") String expr,
      @JsonProperty("nodes") List<String> nodes
  )
  {
    this.type = type;
    this.name = Preconditions.checkNotNull(name, "Missing 'name' in field spec");

    // Validate required fields are present
    switch (type) {
      case ROOT:
        this.expr = (expr == null) ? name : expr;
        this.nodes = null;
        break;

      case TREE:
        this.expr = null;
        Preconditions.checkArgument(
                !CollectionUtils.isNullOrEmpty(nodes),
                "Missing 'nodes' for field[%s], was [%s]", name, nodes);
        this.nodes = nodes;
        break;

      default:
        this.expr = Preconditions.checkNotNull(expr, "Missing 'expr' for field[%s]", name);
        this.nodes = null;
    }
  }

  public JSONPathFieldSpec(
      JSONPathFieldType type,
      String name,
      String expr
  )
  {
    this(type, name, expr, null);
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

  @JsonProperty
  public List<String> getNodes()
  {
    return nodes;
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

  public static JSONPathFieldSpec createTreeField(String name, List<String> nodes)
  {
    return new JSONPathFieldSpec(JSONPathFieldType.TREE, name, null, nodes);
  }

  public static String getCompilePath(String expr)
  {
    return JsonPath.compile(expr).getPath();
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
           Objects.equals(expr, that.expr) &&
           Objects.equals(nodes, that.nodes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, name, expr, nodes);
  }

  @Override
  public String toString()
  {
    return "JSONPathFieldSpec{" +
           "type=" + type +
           ", name='" + name + '\'' +
           ", expr='" + expr + '\'' +
           ", nodes='" + nodes + '\'' +
           '}';
  }
}
