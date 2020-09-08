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

public class NoopIngestionMetrics implements IngestionMetrics
{
  public static final NoopIngestionMetrics INSTANCE = new NoopIngestionMetrics();

  private NoopIngestionMetrics()
  {
  }

  @Override
  public void incrementRowsProcessed(long n)
  {

  }

  @Override
  public void incrementRowsProcessedWithErrors(long n)
  {

  }

  @Override
  public void incrementRowsThrownAway(long n)
  {

  }

  @Override
  public void incrementRowsUnparseable(long n)
  {

  }

  @Override
  public void incrementRowsOut(long numRows)
  {

  }

  @Override
  public void incrementNumPersists(long n)
  {

  }

  @Override
  public void incrementPersistTimeMillis(long millis)
  {

  }

  @Override
  public void incrementPersistBackPressureMillis(long millis)
  {

  }

  @Override
  public void incrementFailedPersists(long n)
  {

  }

  @Override
  public void incrementPersistCpuTime(long persistTime)
  {

  }

  @Override
  public void incrementMergeTimeMillis(long millis)
  {

  }

  @Override
  public void incrementMergeCpuTime(long mergeTime)
  {

  }

  @Override
  public void reportMessageMaxTimestamp(long messageMaxTimestamp)
  {

  }

  @Override
  public IngestionMetricsSnapshot snapshot()
  {
    return NoopIngestionMetricsSnapshot.INSTANCE;
  }
}
