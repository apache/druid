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

package org.apache.druid.segment.incremental;

import java.util.HashMap;
import java.util.Map;

public class SimpleRowIngestionMeters implements RowIngestionMeters
{
  private long processed;
  private long processedWithError;
  private long unparseable;
  private long processedBytes;
  private final long[] thrownAwayByReason = new long[InputRowFilterResult.numValues()];

  @Override
  public long getProcessed()
  {
    return processed;
  }

  @Override
  public void incrementProcessed()
  {
    processed++;
  }

  @Override
  public void incrementProcessedBytes(long increment)
  {
    processedBytes += increment;
  }

  @Override
  public long getProcessedBytes()
  {
    return processedBytes;
  }

  @Override
  public long getProcessedWithError()
  {
    return processedWithError;
  }

  @Override
  public void incrementProcessedWithError()
  {
    processedWithError++;
  }

  @Override
  public long getUnparseable()
  {
    return unparseable;
  }

  @Override
  public void incrementUnparseable()
  {
    unparseable++;
  }

  @Override
  public long getThrownAway()
  {
    long total = 0;
    for (InputRowFilterResult reason : InputRowFilterResult.rejectedValues()) {
      total += thrownAwayByReason[reason.ordinal()];
    }
    return total;
  }

  @Override
  public void incrementThrownAway(InputRowFilterResult reason)
  {
    ++thrownAwayByReason[reason.ordinal()];
  }

  @Override
  public Map<String, Long> getThrownAwayByReason()
  {
    final Map<String, Long> result = new HashMap<>();
    for (InputRowFilterResult reason : InputRowFilterResult.rejectedValues()) {
      long count = thrownAwayByReason[reason.ordinal()];
      if (count > 0) {
        result.put(reason.getReason(), count);
      }
    }
    return result;
  }

  @Override
  public RowIngestionMetersTotals getTotals()
  {
    return new RowIngestionMetersTotals(
        processed,
        processedBytes,
        processedWithError,
        getThrownAwayByReason(),
        unparseable
    );
  }

  @Override
  public Map<String, Object> getMovingAverages()
  {
    throw new UnsupportedOperationException();
  }

  public void addRowIngestionMetersTotals(RowIngestionMetersTotals rowIngestionMetersTotals)
  {
    this.processed += rowIngestionMetersTotals.getProcessed();
    this.processedWithError += rowIngestionMetersTotals.getProcessedWithError();
    this.unparseable += rowIngestionMetersTotals.getUnparseable();
    this.processedBytes += rowIngestionMetersTotals.getProcessedBytes();

    final Map<String, Long> thrownAwayByReason = rowIngestionMetersTotals.getThrownAwayByReason();
    for (InputRowFilterResult reason : InputRowFilterResult.rejectedValues()) {
      this.thrownAwayByReason[reason.ordinal()] += thrownAwayByReason.getOrDefault(reason.getReason(), 0L);
    }
  }
}
