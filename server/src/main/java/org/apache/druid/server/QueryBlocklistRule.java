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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.query.Query;

/**
 * A rule that determines whether a query should be blocked. Implementations define their own
 * matching logic and JSON fields. Use {@link DefaultQueryBlocklistRule} for the standard criteria
 * (datasources, query types, context).
 *
 * <p>Rules with no {@code "type"} field in JSON deserialize as {@link DefaultQueryBlocklistRule}
 * for backwards compatibility. Extensions can register additional implementations as Jackson
 * subtypes via {@code SimpleModule.registerSubtypes(...)}.
 *
 * <p>Implementations must define {@code equals} and {@code hashCode} so that
 * {@link org.apache.druid.server.broker.BrokerDynamicConfig} can detect changes correctly.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultQueryBlocklistRule.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DefaultQueryBlocklistRule.class, name = "default")
})
public interface QueryBlocklistRule
{
  String getRuleName();

  /**
   * Returns true if the query matches this rule and should be blocked.
   */
  boolean matches(Query<?> query);
}
