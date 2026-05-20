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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Information about a SQL query. Implementations are engine-specific.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "engine")
public interface QueryInfo
{
  /**
   * Returns the engine name for this query, matching the JSON "engine" property.
   */
  String engine();

  /**
   * Returns the state of this query, which may be an engine-specific string. Standard strings
   * are in {@link StandardQueryState}, although engines can use additional strings if they like.
   */
  String state();

  /**
   * Returns the execution ID for this query. This is the system-generated ID used internally,
   * such as the dartQueryId for Dart queries.
   */
  String executionId();
}
