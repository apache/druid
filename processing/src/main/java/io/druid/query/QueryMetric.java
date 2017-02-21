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

/**
 * Used in {@link MetricsEmittingQueryRunner} as a binder to a particular metric emitting method of {@link QueryMetrics}
 */
public enum QueryMetric
{
  SEGMENT_TIME {
    @Override
    public void apply(QueryMetrics<?> metrics, long timeNs)
    {
      metrics.segmentTime(timeNs);
    }
  },
  SEGMENT_AND_CACHE_TIME {
    @Override
    public void apply(QueryMetrics<?> metrics, long timeNs)
    {
      metrics.segmentAndCacheTime(timeNs);
    }
  },
  INTERVAL_CHUNK_TIME {
    @Override
    public void apply(QueryMetrics<?> metrics, long timeNs)
    {
      metrics.intervalChunkTime(timeNs);
    }
  };

  public abstract void apply(QueryMetrics<?> metrics, long value);
}
