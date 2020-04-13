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

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * This is a historical occasion that this enum is different from {@link
 * org.apache.druid.server.coordination.ServerType} (also called "node type" in various places) because they are
 * essentially the same abstraction, but merging them could only increase the complexity and drop the code safety,
 * because they name the same types differently ("peon" - "indexer-executor" and "middleManager" - "realtime") and both
 * expose them via JSON APIs.
 *
 * These abstractions can probably be merged when Druid updates to Jackson 2.9 that supports JsonAliases, see
 * see https://github.com/apache/druid/issues/7152.
 */
public enum NodeRole
{
  COORDINATOR("coordinator"),
  HISTORICAL("historical"),
  BROKER("broker"),
  OVERLORD("overlord"),
  PEON("peon"),
  ROUTER("router"),
  MIDDLE_MANAGER("middleManager"),
  INDEXER("indexer");

  private final String jsonName;

  NodeRole(String jsonName)
  {
    this.jsonName = jsonName;
  }

  /**
   * Lowercase for backward compatibility, as a part of the {@link DiscoveryDruidNode}'s JSON format.
   *
   * Don't need to define {@link com.fasterxml.jackson.annotation.JsonCreator} because for enum types {@link JsonValue}
   * serves for both serialization and deserialization, see the Javadoc comment of {@link JsonValue}.
   */
  @JsonValue
  public String getJsonName()
  {
    return jsonName;
  }
}
