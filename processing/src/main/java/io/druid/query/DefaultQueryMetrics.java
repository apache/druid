/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.Interval;

public class DefaultQueryMetrics<QueryType extends BaseQuery<?>> extends AbstractQueryMetrics<QueryType>
{
  public DefaultQueryMetrics(ObjectMapper jsonMapper)
  {
    super(jsonMapper);
  }

  @Override
  public void query(QueryType query)
  {
    dataSource(query);
    queryType(query);
    interval(query);
    hasFilters(query);
    duration(query);
    queryId(query);
  }

  /**
   * Sets {@link BaseQuery#getDataSource()} of the given query as dimension.
   */
  public void dataSource(QueryType query)
  {
    builder.setDimension(
        DruidMetrics.DATASOURCE,
        DataSourceUtil.getMetricName(query.getDataSource())
    );
  }

  /**
   * Sets {@link BaseQuery#getIntervals()} of the given query as dimension.
   */
  public void interval(QueryType query)
  {
    builder.setDimension(
        DruidMetrics.INTERVAL,
        query.getIntervals().stream()
             .map(Interval::toString).toArray(String[]::new)
    );
  }
}
