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

/**
 * Monitors and emits the metrics corresponding to the subqueries and their materialization.
 */
public class SubqueryCountStatsMonitor extends AbstractMonitor
{

  private static final String KEY = "subqueryCountStats";

  private static final String ROW_LIMIT_COUNT = "subquery/rowLimit/count";
  private static final String BYTE_LIMIT_COUNT = "subquery/byteLimit/count";
  private static final String FALLBACK_COUNT = "subquery/fallback/count";
  private static final String INSUFFICIENT_TYPE_COUNT = "subquery/fallback/insufficientType/count";
  private static final String UNKNOWN_REASON_COUNT = "subquery/fallback/unknownReason/count";
  private static final String ROW_LIMIT_EXCEEDED_COUNT = "query/rowLimit/exceeded/count";
  private static final String BYTE_LIMIT_EXCEEDED_COUNT = "query/byteLimit/exceeded/count";

  private final KeyedDiff keyedDiff = new KeyedDiff();
  private final SubqueryCountStatsProvider statsProvider;

  @Inject
  public SubqueryCountStatsMonitor(SubqueryCountStatsProvider statsProvider)
  {
    this.statsProvider = statsProvider;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    final long subqueriesWithRowBasedLimit = statsProvider.subqueriesWithRowLimit();
    final long subqueriesWithByteBasedLimit = statsProvider.subqueriesWithByteLimit();
    final long subqueriesFallingBackToRowBasedLimit = statsProvider.subqueriesFallingBackToRowLimit();
    final long subqueriesFallingBackDueToUnsufficientTypeInfo = statsProvider.subqueriesFallingBackDueToUnsufficientTypeInfo();
    final long subqueriesFallingBackDueToUnknownReason = statsProvider.subqueriesFallingBackDueUnknownReason();
    final long queriesExceedingRowLimit = statsProvider.queriesExceedingRowLimit();
    final long queriesExceedingByteLimit = statsProvider.queriesExceedingByteLimit();

    Map<String, Long> diff = keyedDiff.to(
        KEY,
        ImmutableMap.of(
            ROW_LIMIT_COUNT, subqueriesWithRowBasedLimit,
            BYTE_LIMIT_COUNT, subqueriesWithByteBasedLimit,
            FALLBACK_COUNT, subqueriesFallingBackToRowBasedLimit,
            INSUFFICIENT_TYPE_COUNT, subqueriesFallingBackDueToUnsufficientTypeInfo,
            UNKNOWN_REASON_COUNT, subqueriesFallingBackDueToUnknownReason,
            ROW_LIMIT_EXCEEDED_COUNT, queriesExceedingRowLimit,
            BYTE_LIMIT_EXCEEDED_COUNT, queriesExceedingByteLimit
        )
    );

    if (diff != null) {
      for (Map.Entry<String, Long> diffEntry : diff.entrySet()) {
        emitter.emit(builder.setMetric(diffEntry.getKey(), diffEntry.getValue()));
      }
    }

    return true;
  }
}
