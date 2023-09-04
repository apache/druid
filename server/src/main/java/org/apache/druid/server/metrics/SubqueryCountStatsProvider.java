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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects the metrics corresponding to the subqueries and their materialization.
 */
public class SubqueryCountStatsProvider
{

  private final AtomicLong successfulSubqueriesWithRowLimit = new AtomicLong();
  private final AtomicLong successfulSubqueriesWithByteLimit = new AtomicLong();
  private final AtomicLong subqueriesFallingBackToRowLimit = new AtomicLong();
  private final AtomicLong subqueriesFallingBackDueToUnsufficientTypeInfo = new AtomicLong();
  private final AtomicLong subqueriesFallingBackDueToUnknownReason = new AtomicLong();
  private final AtomicLong queriesExceedingRowLimit = new AtomicLong();
  private final AtomicLong queriesExceedingByteLimit = new AtomicLong();

  /**
   * @return Count of subqueries where the results are materialized as rows i.e. {@code List<Object>}
   */
  public long subqueriesWithRowLimit()
  {
    return successfulSubqueriesWithRowLimit.get();
  }

  /**
   * @return Count of subqueries where the results are materialized as {@link org.apache.druid.frame.Frame}
   */
  public long subqueriesWithByteLimit()
  {
    return successfulSubqueriesWithByteLimit.get();
  }

  /**
   * @return Count of subqueries where the results are
   */
  public long subqueriesFallingBackToRowLimit()
  {
    return subqueriesFallingBackToRowLimit.get();
  }

  /**
   * @return Count of the subset of subqueries that are falling back due to insufficient type information in the
   * {@link org.apache.druid.segment.column.RowSignature}. This is expected to be the most common and already known
   * cause of fallback, therefore this is added as a separate metric
   */
  public long subqueriesFallingBackDueToUnsufficientTypeInfo()
  {
    return subqueriesFallingBackDueToUnsufficientTypeInfo.get();
  }

  /**
   * @return Count of the subset of subqueries that are falling back due to insufficient an unknown error. This can be due to a
   * few known reasons like columnar frames not supporting the array types right now, or due to unknown errors while
   * performing the materialization
   */
  public long subqueriesFallingBackDueUnknownReason()
  {
    return subqueriesFallingBackDueToUnknownReason.get();
  }

  /**
   * @return Number of queries that fail due to their subqueries exceeding the prescribed row limit
   */
  public long queriesExceedingRowLimit()
  {
    return queriesExceedingRowLimit.get();
  }

  /**
   * @return Number of subqueries that fail due to their subqueries exceeding the prescribed byte limit
   */
  public long queriesExceedingByteLimit()
  {
    return queriesExceedingByteLimit.get();
  }


  public void incrementSubqueriesWithRowLimit()
  {
    successfulSubqueriesWithRowLimit.incrementAndGet();
  }

  public void incrementSubqueriesWithByteLimit()
  {
    successfulSubqueriesWithByteLimit.incrementAndGet();
  }

  public void incrementSubqueriesFallingBackToRowLimit()
  {
    subqueriesFallingBackToRowLimit.incrementAndGet();
  }

  public void incrementSubqueriesFallingBackDueToUnsufficientTypeInfo()
  {
    subqueriesFallingBackDueToUnsufficientTypeInfo.incrementAndGet();
  }

  public void incrementSubqueriesFallingBackDueToUnknownReason()
  {
    subqueriesFallingBackDueToUnknownReason.incrementAndGet();
  }

  public void incrementQueriesExceedingRowLimit()
  {
    queriesExceedingRowLimit.incrementAndGet();
  }

  public void incrementQueriesExceedingByteLimit()
  {
    queriesExceedingByteLimit.incrementAndGet();
  }
}
