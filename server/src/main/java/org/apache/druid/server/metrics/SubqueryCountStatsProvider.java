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

public class SubqueryCountStatsProvider
{

  private final AtomicLong successfulSubqueriesWithRowBasedLimit = new AtomicLong();
  private final AtomicLong successfulSubqueriesWithByteBasedLimit = new AtomicLong();
  private final AtomicLong subqueriesFallingBackToRowBasedLimit = new AtomicLong();
  private final AtomicLong subqueriesFallingBackDueToUnsufficientTypeInfo = new AtomicLong();

  /**
   * @return Count of subqueries where the results are materialized as rows {@code List<Object>}
   */
  public long subqueriesWithRowBasedLimit()
  {
    return successfulSubqueriesWithRowBasedLimit.get();
  }

  /**
   * @return Count of subqueries where the results are materialized as {@link org.apache.druid.frame.Frame}
   */
  public long subqueriesWithByteBasedLimit()
  {
    return successfulSubqueriesWithByteBasedLimit.get();
  }

  /**
   * @return Count of subqueries where the results are materialized as
   */
  public long subqueriesFallingBackToRowBasedLimit()
  {
    return subqueriesFallingBackToRowBasedLimit.get();
  }

  /**
   * @return Subset of subqueries that are falling back, which fall back due to insufficient type information in the
   * {@link org.apache.druid.segment.column.RowSignature}. This is expected to be the most common and already known
   * cause of fallback, therefore this is added as a separate metric, so that we can know
   */
  public long subqueriesFallingBackDueToUnsufficientTypeInfo()
  {
    return subqueriesFallingBackDueToUnsufficientTypeInfo.get();
  }

  public void incrementSubqueriesWithRowBasedLimit()
  {
    successfulSubqueriesWithRowBasedLimit.incrementAndGet();
  }

  public void incrementSubqueriesWithByteBasedLimit()
  {
    successfulSubqueriesWithByteBasedLimit.incrementAndGet();
  }

  public void incrementSubqueriesFallingBackToRowBasedLimit()
  {
    subqueriesFallingBackToRowBasedLimit.incrementAndGet();
  }

  public void incrementSubqueriesFallingBackDueToUnsufficientTypeInfo()
  {
    subqueriesFallingBackDueToUnsufficientTypeInfo.incrementAndGet();
  }
}
