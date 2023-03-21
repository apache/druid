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

package org.apache.druid.msq.counters;

import org.apache.druid.segment.realtime.FireDepartmentMetrics;

/**
 * Wrapper around {@link FireDepartmentMetrics} which updates the progress counters while updating its metrics. This
 * is necessary as the {@link org.apache.druid.segment.realtime.appenderator.BatchAppenderator} used by the
 * {@link org.apache.druid.msq.indexing.SegmentGeneratorFrameProcessor} is not part of the MSQ extension, and hence,
 * cannot update the counters used in MSQ reports as it persists and pushes segments to deep storage.
 */
public class SegmentGeneratorMetricsWrapper extends FireDepartmentMetrics
{
  private final SegmentGenerationProgressCounter segmentGenerationProgressCounter;

  public SegmentGeneratorMetricsWrapper(SegmentGenerationProgressCounter segmentGenerationProgressCounter)
  {
    this.segmentGenerationProgressCounter = segmentGenerationProgressCounter;
  }

  @Override
  public void incrementRowOutputCount(long numRows)
  {
    super.incrementRowOutputCount(numRows);
    segmentGenerationProgressCounter.incrementRowsPersisted(numRows);
  }

  @Override
  public void incrementMergedRows(long rows)
  {
    super.incrementMergedRows(rows);
    segmentGenerationProgressCounter.incrementRowsMerged(rows);
  }

  @Override
  public void incrementPushedRows(long rows)
  {
    super.incrementPushedRows(rows);
    segmentGenerationProgressCounter.incrementRowsPushed(rows);
  }
}
