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

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.Nullable;

public class NoopIngestionMetricsSnapshot implements IngestionMetricsSnapshot
{
  public static final String TYPE = "noop";
  public static final NoopIngestionMetricsSnapshot INSTANCE = new NoopIngestionMetricsSnapshot();

  @JsonCreator
  private NoopIngestionMetricsSnapshot()
  {
  }

  @Override
  public IngestionMetricsSnapshot diff(@Nullable IngestionMetricsSnapshot prevSnapshot)
  {
    return this;
  }

  @Override
  public long getRowsProcessed()
  {
    return 0;
  }

  @Override
  public long getRowsProcessedWithErrors()
  {
    return 0;
  }

  @Override
  public long getRowsThrownAway()
  {
    return 0;
  }

  @Override
  public long getRowsUnparseable()
  {
    return 0;
  }

  @Override
  public long getRowsOut()
  {
    return 0;
  }

  @Override
  public long getNumPersists()
  {
    return 0;
  }

  @Override
  public long getPersistTimeMillis()
  {
    return 0;
  }

  @Override
  public long getPersistBackPressureMillis()
  {
    return 0;
  }

  @Override
  public long getFailedPersists()
  {
    return 0;
  }

  @Override
  public long getMergeTimeMillis()
  {
    return 0;
  }

  @Override
  public long getMergeCpuTime()
  {
    return 0;
  }

  @Override
  public long getPersistCpuTime()
  {
    return 0;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj instanceof NoopIngestionMetricsSnapshot;
  }
}
