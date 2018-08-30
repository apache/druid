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

package org.apache.druid.query.groupby;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.QueryMetrics;

/**
 * Specialization of {@link QueryMetrics} for {@link GroupByQuery}.
 */
@ExtensionPoint
public interface GroupByQueryMetrics extends QueryMetrics<GroupByQuery>
{
  /**
   * Sets the size of {@link GroupByQuery#getDimensions()} of the given query as dimension.
   */
  @PublicApi
  void numDimensions(GroupByQuery query);

  /**
   * Sets the number of metrics of the given groupBy query as dimension.
   */
  @PublicApi
  void numMetrics(GroupByQuery query);

  /**
   * Sets the number of "complex" metrics of the given groupBy query as dimension. By default it is assumed that
   * "complex" metric is a metric of not long or double type, but it could be redefined in the implementation of this
   * method.
   */
  @PublicApi
  void numComplexMetrics(GroupByQuery query);

  /**
   * Sets the granularity of {@link GroupByQuery#getGranularity()} of the given query as dimension.
   */
  @PublicApi
  void granularity(GroupByQuery query);
}
