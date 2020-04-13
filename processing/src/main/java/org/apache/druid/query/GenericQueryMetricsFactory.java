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

import org.apache.druid.guice.annotations.PublicApi;

/**
 * This factory is used for DI of custom {@link QueryMetrics} implementations for all query types, which don't (yet)
 * need to emit custom dimensions and/or metrics, i. e. they are good with the generic {@link QueryMetrics} interface.
 *
 * Implementations could be injected using
 *
 * PolyBind
 *    .optionBinder(binder, Key.get(GenericQueryMetricsFactory.class))
 *    .addBinding("myCustomGenericQueryMetricsFactory")
 *    .to(MyCustomGenericQueryMetricsFactory.class);
 *
 * And then setting property:
 * druid.query.generic.queryMetricsFactory=myCustomGenericQueryMetricsFactory
 *
 * Unlike {@link QueryMetrics} itself, this interface is considered stable and is expected to be injected into custom
 * Query extensions that do not want to worry about the potential instability of {@link QueryMetrics}.
 */
@PublicApi
public interface GenericQueryMetricsFactory
{
  /**
   * Creates a {@link QueryMetrics} for query, which doesn't have predefined QueryMetrics subclass. This method must
   * call {@link QueryMetrics#query(Query)} with the given query on the created QueryMetrics object before returning.
   */
  QueryMetrics<Query<?>> makeMetrics(Query<?> query);
}
