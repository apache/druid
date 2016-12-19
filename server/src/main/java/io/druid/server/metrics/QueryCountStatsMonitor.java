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
package io.druid.server.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;
import com.metamx.metrics.KeyedDiff;

import java.util.Map;

public class QueryCountStatsMonitor extends AbstractMonitor
{
  private final KeyedDiff keyedDiff = new KeyedDiff();
  private final QueryCountStatsProvider statsProvider;

  @Inject
  public QueryCountStatsMonitor(
      QueryCountStatsProvider statsProvider
  )
  {
    this.statsProvider = statsProvider;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    Map<String, Long> diff = keyedDiff.to(
        "queryCountStats",
        ImmutableMap.of("query/success/count", statsProvider.getSuccessfulQueryCount(),
                        "query/failed/count", statsProvider.getFailedQueryCount(),
                        "query/interrupted/count", statsProvider.getInterruptedQueryCount()
        )
    );
    if (diff != null) {
      for (Map.Entry<String, Long> diffEntry : diff.entrySet()) {
        emitter.emit(builder.build(diffEntry.getKey(), diffEntry.getValue()));
      }
    }
    return true;
  }

}
