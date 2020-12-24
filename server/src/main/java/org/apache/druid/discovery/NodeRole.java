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

package org.apache.druid.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Defines the 'role' of a Druid service, utilized to strongly type announcement and service discovery.
 *
 * Originally, this was an enum to add type safety for discovery and announcement purposes, but was expanded
 * into a class to allow for extensibility while retaining the type safety of using defined types instead of raw
 * strings. As such, this class tries to mimic the interface provided by the previous enum.
 *
 * Built in node roles define a {@link #name} that is distinct from {@link #jsonName}, and is the previous value
 * which would occur when the enum was used in a 'toString' context. Custom node roles allow extension to participate
 * in announcement and discovery, but are limited to only using {@link #jsonName} for both toString and JSON serde.
 *
 * The historical context of why the enum was different from {@link org.apache.druid.server.coordination.ServerType}
 * (also called "node type" in various places) is because while they are essentially the same abstraction, merging them
 * could only increase the complexity and drop the code safety, because they name the same types differently
 * ("peon" - "indexer-executor" and "middleManager" - "realtime") and both expose them via JSON APIs.
 *
 * These abstractions can all potentially be merged when Druid updates to Jackson 2.9 that supports JsonAliases,
 * see https://github.com/apache/druid/issues/7152.
 */
public class NodeRole
{
  public static final NodeRole COORDINATOR = new NodeRole("COORDINATOR", "coordinator");
  public static final NodeRole HISTORICAL = new NodeRole("HISTORICAL", "historical");
  public static final NodeRole BROKER = new NodeRole("BROKER", "broker");
  public static final NodeRole OVERLORD = new NodeRole("OVERLORD", "overlord");
  public static final NodeRole PEON = new NodeRole("PEON", "peon");
  public static final NodeRole ROUTER = new NodeRole("ROUTER", "router");
  public static final NodeRole MIDDLE_MANAGER = new NodeRole("MIDDLE_MANAGER", "middleManager");
  public static final NodeRole INDEXER = new NodeRole("INDEXER", "indexer");

  private static final NodeRole[] BUILT_IN = new NodeRole[]{
      COORDINATOR,
      HISTORICAL,
      BROKER,
      OVERLORD,
      PEON,
      ROUTER,
      MIDDLE_MANAGER,
      INDEXER
  };

  private static final Map<String, NodeRole> BUILT_IN_LOOKUP =
      Arrays.stream(BUILT_IN).collect(Collectors.toMap(NodeRole::getJsonName, Function.identity()));

  /**
   * For built-in roles, to preserve backwards compatibility when this was an enum, this provides compatibility for
   * usages of the enum name as a string, (e.g. allcaps 'COORDINATOR'), which is used by system tables for displaying
   * node role, and by curator discovery for the discovery path of a node role (the actual payload at the zk location
   * uses {@link #jsonName})
   */
  private final String name;

  /**
   * JSON serialized value for {@link NodeRole}
   */
  private final String jsonName;

  /**
   * Create a custom node role. Known Druid node roles should ALWAYS use the built-in static node roles:
   * ({@link #COORDINATOR}, {@link #OVERLORD}, {@link #ROUTER}, {@link #BROKER}{@link #INDEXER},
   * {@link #MIDDLE_MANAGER}, {@link #HISTORICAL}) instead of constructing a new instance.
   */
  public NodeRole(String jsonName)
  {
    this(jsonName, jsonName);
  }

  /**
   * for built-in roles, to preserve backwards compatibility when this was an enum, allow built-in node roles to specify
   * the 'name' which is used by 'toString' to be separate from the jsonName, which is the value which the node role
   * will be serialized as and deserialized from
   */
  private NodeRole(String name, String jsonName)
  {
    this.name = name;
    this.jsonName = jsonName;
  }

  @JsonValue
  public String getJsonName()
  {
    return jsonName;
  }

  @JsonCreator
  public static NodeRole fromJsonName(String jsonName)
  {
    return BUILT_IN_LOOKUP.getOrDefault(jsonName, new NodeRole(jsonName));
  }

  @Override
  public String toString()
  {
    // for built-in roles, to preserve backwards compatibility when this was an enum
    return name;
  }

  /**
   * built-in node roles
   */
  public static NodeRole[] values()
  {
    return Arrays.copyOf(BUILT_IN, BUILT_IN.length);
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
    NodeRole nodeRole = (NodeRole) o;
    return name.equals(nodeRole.name) && jsonName.equals(nodeRole.jsonName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, jsonName);
  }
}
