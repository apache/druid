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

package org.apache.druid.indexing.compact;

import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionSlotManager;
import org.apache.druid.server.compaction.DataSourceCompactibleSegmentIterator;
import org.apache.druid.server.compaction.NewestSegmentFirstPolicy;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This template never needs to be deserialized as a {@code BatchIndexingJobTemplate}.
 * It is just a delegating template that uses a {@link DataSourceCompactionConfig}
 * to create compaction jobs.
 */
public class CompactionConfigBasedJobTemplate implements CompactionJobTemplate
{
  private final DataSourceCompactionConfig config;

  public CompactionConfigBasedJobTemplate(DataSourceCompactionConfig config)
  {
    this.config = config;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    return config.getSegmentGranularity();
  }

  @Override
  public List<CompactionJob> createCompactionJobs(
      DruidInputSource source,
      CompactionJobParams params
  )
  {
    final DataSourceCompactibleSegmentIterator segmentIterator = getCompactibleCandidates(source, params);

    final List<CompactionJob> jobs = new ArrayList<>();

    // Create a job for each CompactionCandidate
    while (segmentIterator.hasNext()) {
      final CompactionCandidate candidate = segmentIterator.next();

      ClientCompactionTaskQuery taskPayload
          = CompactSegments.createCompactionTask(candidate, config, params.getClusterCompactionConfig().getEngine());
      jobs.add(
          new CompactionJob(
              taskPayload,
              candidate,
              CompactionSlotManager.computeSlotsRequiredForTask(taskPayload)
          )
      );
    }

    return jobs;
  }

  @Override
  public String getType()
  {
    throw DruidException.defensive(
        "This template cannot be serialized. It is an adapter used to create jobs"
        + " using a legacy DataSourceCompactionConfig. Do not use this template"
        + " in a supervisor spec directly. Use types [compactCatalog], [compactMsq]"
        + " or [compactInline] instead."
    );
  }

  /**
   * Creates an iterator over the compactible candidate segments for the given
   * params. Adds stats for segments that are already compacted to the
   * {@link CompactionJobParams#getSnapshotBuilder()}.
   */
  DataSourceCompactibleSegmentIterator getCompactibleCandidates(
      DruidInputSource source,
      CompactionJobParams params
  )
  {
    validateInput(source);

    final Interval searchInterval = Objects.requireNonNull(source.getInterval());

    final SegmentTimeline timeline = params.getTimeline(config.getDataSource());
    final DataSourceCompactibleSegmentIterator iterator = new DataSourceCompactibleSegmentIterator(
        config,
        timeline,
        Intervals.complementOf(searchInterval),
        new NewestSegmentFirstPolicy(null)
    );

    // Collect stats for segments that are already compacted
    iterator.getCompactedSegments().forEach(entry -> params.getSnapshotBuilder().addToComplete(entry));

    return iterator;
  }

  private void validateInput(DruidInputSource druidInputSource)
  {
    if (!druidInputSource.getDataSource().equals(config.getDataSource())) {
      throw InvalidInput.exception(
          "Datasource[%s] in compaction config does not match datasource[%s] in input source",
          config.getDataSource(), druidInputSource.getDataSource()
      );
    }
  }
}
