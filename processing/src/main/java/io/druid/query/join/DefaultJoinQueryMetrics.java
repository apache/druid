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

package io.druid.query.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.DataSourceUtil;
import io.druid.query.DataSourceWithSegmentSpec;
import io.druid.query.DefaultQueryMetrics;
import io.druid.query.DruidMetrics;

public class DefaultJoinQueryMetrics extends DefaultQueryMetrics<JoinQuery> implements JoinQueryMetrics
{
  public DefaultJoinQueryMetrics(ObjectMapper jsonMapper)
  {
    super(jsonMapper);
  }

  @Override
  public void query(JoinQuery query)
  {
    queryType(query);
    distributionTarget(query);
    distributionTargetDuration(query);
    numDataSources(query);
    hasFilters(query);
    queryId(query);
  }

  @Override
  public void dataSourcesAndDurations(JoinQuery query)
  {
    builder.setDimension(
        "dataSourcesAndDurations",
        query.getDataSources().stream()
             .map(
                 spec -> "{dataSource=" + DataSourceUtil.getMetricName(spec.getDataSource())
                         + ",duration=" + query.getDuration(spec.getDataSource()) + "}"
             )
             .toArray(String[]::new)
    );
  }

  @Override
  public void intervals(JoinQuery query)
  {
    builder.setDimension(
        DruidMetrics.INTERVAL,
        query.getDataSources().stream()
             .map(spec -> spec.getQuerySegmentSpec().getIntervals().toString())
             .toArray(String[]::new)
    );
  }

  @Override
  public void numDataSources(JoinQuery query)
  {
    builder.setDimension("numDataSources", String.valueOf(query.getDataSources().size()));
  }

  @Override
  public void distributionTarget(JoinQuery query)
  {
    final DataSourceWithSegmentSpec distributionTarget = query.getDistributionTarget();
    builder.setDimension(
        "distributionTarget",
        distributionTarget == null ? "" : DataSourceUtil.getMetricName(distributionTarget.getDataSource())
    );
  }

  @Override
  public void distributionTargetDuration(JoinQuery query)
  {
    final DataSourceWithSegmentSpec distributionTarget = query.getDistributionTarget();
    builder.setDimension(
        "distributionTargetDuration",
        distributionTarget == null ? "" : query.getDuration(distributionTarget.getDataSource()).toString()
    );
  }
}
