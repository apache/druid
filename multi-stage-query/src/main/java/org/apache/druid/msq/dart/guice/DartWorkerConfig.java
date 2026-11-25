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

package org.apache.druid.msq.dart.guice;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.dart.worker.DartWorkerRunner;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.querykit.ReadableInputQueue;
import org.joda.time.Period;

import javax.annotation.Nullable;

/**
 * Runtime configuration for workers (which run on Historicals).
 */
public class DartWorkerConfig
{
  /**
   * By default, allocate up to 35% of memory for the MSQ framework. This accounts for additional overhead due to
   * native queries, and lookups (which aren't accounted for by the Dart {@link MemoryIntrospector}).
   */
  private static final double DEFAULT_HEAP_FRACTION = 0.35;

  private static final Period DEFAULT_CONTROLLER_TIMEOUT = Period.minutes(3);
  private static final Period DEFAULT_SYNC_TIMEOUT = Period.seconds(30);

  /**
   * Minimum value for {@link #getControllerTimeout()}. Any shorter and we might end up reaping controllers in between
   * heartbeats.
   */
  public static final long MIN_CONTROLLER_TIMEOUT_MILLIS = 60_000;

  public static final int AUTO = -1;

  @JsonProperty("concurrentQueries")
  private final int concurrentQueries;

  @JsonProperty("heapFraction")
  private final double heapFraction;

  @JsonProperty("segmentLoadAheadCount")
  @Nullable
  private final Integer segmentLoadAheadCount;

  @JsonProperty("controllerTimeout")
  private final Period controllerTimeout;

  @JsonProperty("syncTimeout")
  private final Period syncTimeout;

  /**
   * Creates a config. Null parameters take their default values.
   */
  @JsonCreator
  public DartWorkerConfig(
      @JsonProperty("concurrentQueries") @Nullable final Integer concurrentQueries,
      @JsonProperty("heapFraction") @Nullable final Double heapFraction,
      @JsonProperty("segmentLoadAheadCount") @Nullable final Integer segmentLoadAheadCount,
      @JsonProperty("controllerTimeout") @Nullable final Period controllerTimeout,
      @JsonProperty("syncTimeout") @Nullable final Period syncTimeout
  )
  {
    this.concurrentQueries = Configs.valueOrDefault(concurrentQueries, AUTO);
    this.heapFraction = Configs.valueOrDefault(heapFraction, DEFAULT_HEAP_FRACTION);
    this.segmentLoadAheadCount = segmentLoadAheadCount;
    this.controllerTimeout = Configs.valueOrDefault(controllerTimeout, DEFAULT_CONTROLLER_TIMEOUT);
    this.syncTimeout = Configs.valueOrDefault(syncTimeout, DEFAULT_SYNC_TIMEOUT);

    if (this.controllerTimeout.toStandardDuration().getMillis() < MIN_CONTROLLER_TIMEOUT_MILLIS) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "druid.msq.dart.worker.controllerTimeout[%s] must be at least [%,d] ms",
                              this.controllerTimeout,
                              MIN_CONTROLLER_TIMEOUT_MILLIS
                          );
    }

    if (this.syncTimeout.toStandardDuration().getMillis() <= 0) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("druid.msq.dart.worker.syncTimeout[%s] must be positive", this.syncTimeout);
    }
  }

  /**
   * Returns the maximum number of workers that can run concurrently on this server, or {@link #AUTO} to use the
   * number of merge buffers.
   */
  public int getConcurrentQueries()
  {
    return concurrentQueries;
  }

  /**
   * Returns the fraction of heap available to the MSQ framework, across all workers on this server.
   */
  public double getHeapFraction()
  {
    return heapFraction;
  }

  /**
   * Returns the worker-local value for the segment load-ahead count used to size segment prefetch in
   * {@link ReadableInputQueue}.
   * <p>
   * Defaults to null (unset), which leaves the value to query context (client or controller-default supplied) and the
   * built-in {@code 2 * threadCount} fallback. When set to a positive value, it acts as a worker-local default used
   * only when the query context does not supply a value.
   * <p>
   * This is per-worker-process configuration, set independently on each worker. It lets workers with different
   * hardware (for example, separate tiers with more or less memory and storage bandwidth) tune their own prefetch
   * depth, rather than relying solely on a single cluster-wide query-context default.
   */
  @Nullable
  public Integer getSegmentLoadAheadCount()
  {
    return segmentLoadAheadCount;
  }

  /**
   * Returns how long a controller can go without contacting this worker before {@link DartWorkerRunner} considers it
   * gone. Always at least {@link #MIN_CONTROLLER_TIMEOUT_MILLIS}.
   */
  public Period getControllerTimeout()
  {
    return controllerTimeout;
  }

  /**
   * Returns how long a query can be missing from a controller's syncs before {@link DartWorkerRunner} cancels it.
   * Always positive.
   */
  public Period getSyncTimeout()
  {
    return syncTimeout;
  }
}
