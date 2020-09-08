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

package org.apache.druid.indexing.stats;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

import javax.annotation.Nullable;

/**
 * A snapshot of {@link IngestionMetrics}. Implmentions must be immutable.
 */
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = NoopIngestionMetricsSnapshot.TYPE, value = NoopIngestionMetricsSnapshot.class)
})
public interface IngestionMetricsSnapshot
{
  /**
   * Computes a diff between {@param prevSnapshot} and this. This method is mostly used in
   * {@link org.apache.druid.java.util.metrics.Monitor}.
   * It is allowed to return {@code this} if {@param prevSnapshot} is null.
   */
  IngestionMetricsSnapshot diff(@Nullable IngestionMetricsSnapshot prevSnapshot);

  long getRowsProcessed();

  long getRowsProcessedWithErrors();

  long getRowsThrownAway();

  long getRowsUnparseable();

  long getRowsOut();

  long getNumPersists();

  long getPersistTimeMillis();

  long getPersistBackPressureMillis();

  long getFailedPersists();

  long getMergeTimeMillis();

  long getMergeCpuTime();

  long getPersistCpuTime();
}
