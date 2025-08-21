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

package org.apache.druid.msq.counters;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface for the results of {@link QueryCounter#snapshot()}. No methods, because the only purpose of these
 * snapshots is to pass things along from worker -> controller -> report.
 *
 * To support easy adding of new counters, implementations must use forward-compatible deserialization setups.
 * In particular, implementations should avoid using enums where new values may be added in the future.
 *
 * The default impl is {@link NilQueryCounterSnapshot}. This means that readers will see {@link NilQueryCounterSnapshot}
 * if they don't understand the particular counter type in play.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NilQueryCounterSnapshot.class)
public interface QueryCounterSnapshot
{
}
