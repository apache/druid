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

package org.apache.druid.query;

import java.util.Map;

/**
 * Provides the default query context applied to all incoming queries before per-query overrides are merged in.
 *
 * <p>On non-broker nodes this is backed by static runtime properties ({@link DefaultQueryConfig}).
 * On brokers it is backed by {@code BrokerViewOfBrokerConfig}, which merges the static defaults with
 * operator-supplied overrides pushed dynamically from the Coordinator.
 */
public interface QueryContextProvider
{
  Map<String, Object> getContext();
}
