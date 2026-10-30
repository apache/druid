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

import java.util.concurrent.TimeUnit;

/**
 * Test-only {@link QueryMetrics} that emits every metric {@link DefaultQueryMetrics} disables by default.
 * Production keeps those methods as no-ops, but tests that need to assert on any of them can use this implementation
 * (or {@link Factory}) instead.
 */
public class EmittingQueryMetrics<QueryType extends Query<?>> extends DefaultQueryMetrics<QueryType>
{
  @Override
  public QueryMetrics<QueryType> reportBackPressureTime(long timeNs)
  {
    return reportMetric("query/node/backpressure", TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportBitmapConstructionTime(long timeNs)
  {
    return reportMetric("query/segment/bitmap/constructionTime", TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentRows(long numRows)
  {
    return reportMetric("query/segment/rows", numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportPreFilteredRows(long numRows)
  {
    return reportMetric("query/segment/preFilteredRows", numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeParallelism(int parallelism)
  {
    return reportMetric("query/parallelMerge/parallelism", parallelism);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputSequences(long numSequences)
  {
    return reportMetric("query/parallelMerge/inputSequences", numSequences);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputRows(long numRows)
  {
    return reportMetric("query/parallelMerge/inputRows", numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeOutputRows(long numRows)
  {
    return reportMetric("query/parallelMerge/outputRows", numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTaskCount(long numTasks)
  {
    return reportMetric("query/parallelMerge/taskCount", numTasks);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalCpuTime(long timeNs)
  {
    return reportMetric("query/parallelMerge/cpuTime", TimeUnit.NANOSECONDS.toMicros(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalTime(long timeNs)
  {
    return reportMetric("query/parallelMerge/time", TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeFastestPartitionTime(long timeNs)
  {
    return reportMetric("query/parallelMerge/fastestPartitionTime", TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeSlowestPartitionTime(long timeNs)
  {
    return reportMetric("query/parallelMerge/slowestPartitionTime", TimeUnit.NANOSECONDS.toMillis(timeNs));
  }

  @Override
  public QueryMetrics<QueryType> reportQueriedSegmentCount(long segmentCount)
  {
    return reportMetric(QUERY_SEGMENTS_COUNT, segmentCount);
  }

  /**
   * {@link GenericQueryMetricsFactory} producing {@link EmittingQueryMetrics}.
   */
  public static class Factory implements GenericQueryMetricsFactory
  {
    @Override
    public QueryMetrics<Query<?>> makeMetrics(Query<?> query)
    {
      final EmittingQueryMetrics<Query<?>> queryMetrics = new EmittingQueryMetrics<>();
      queryMetrics.query(query);
      return queryMetrics;
    }

    @Override
    public QueryMetrics<Query<?>> makeMetrics()
    {
      return new EmittingQueryMetrics<>();
    }
  }
}
