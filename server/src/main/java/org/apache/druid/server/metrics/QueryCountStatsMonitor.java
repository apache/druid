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

package org.apache.druid.server.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.KeyedDiff;

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
    final long successfulQueryCount = statsProvider.getSuccessfulQueryCount();
    final long failedQueryCount = statsProvider.getFailedQueryCount();
    final long interruptedQueryCount = statsProvider.getInterruptedQueryCount();
    Map<String, Long> diff = keyedDiff.to(
        "queryCountStats",
        ImmutableMap.of(
            "query/count", successfulQueryCount + failedQueryCount + interruptedQueryCount,
            "query/success/count", successfulQueryCount,
            "query/failed/count", failedQueryCount,
            "query/interrupted/count", interruptedQueryCount
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
